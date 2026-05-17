import net from "node:net";

const host = process.env.OPENDB_PGWIRE_HOST ?? "127.0.0.1";
const port = Number.parseInt(process.env.OPENDB_PGWIRE_PORT ?? "5432", 10);

const client = new net.Socket();
let buffer = Buffer.alloc(0);
const tableName = `accounts_${Date.now()}_${process.pid}`;

client.setTimeout(5_000);

function connect(): Promise<void> {
  return new Promise((resolve, reject) => {
    const fail = (error: Error) => {
      cleanup();
      client.destroy();
      reject(error);
    };
    const cleanup = () => {
      client.off("connect", onConnect);
      client.off("error", onError);
      client.off("timeout", onTimeout);
      client.off("close", onClose);
    };
    const onConnect = () => {
      cleanup();
      resolve();
    };
    const onError = (error: Error) => fail(error);
    const onTimeout = () => fail(new Error(`timed out connecting to ${host}:${port}`));
    const onClose = () => fail(new Error(`connection closed before connecting to ${host}:${port}`));

    client.once("connect", onConnect);
    client.once("error", onError);
    client.once("timeout", onTimeout);
    client.once("close", onClose);
    client.connect({ host, port });
  });
}

function readExactly(length: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const onData = () => {
      if (buffer.length < length) {
        return;
      }
      cleanup();
      const chunk = buffer.subarray(0, length);
      buffer = buffer.subarray(length);
      resolve(chunk);
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      client.off("data", onChunk);
      client.off("error", onError);
      client.off("timeout", onTimeout);
      client.off("close", onClose);
    };
    const onChunk = (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      onData();
    };
    const onTimeout = () => onError(new Error("timed out waiting for pgwire data"));
    const onClose = () => onError(new Error("connection closed while waiting for pgwire data"));

    client.on("data", onChunk);
    client.once("error", onError);
    client.once("timeout", onTimeout);
    client.once("close", onClose);
    onData();
  });
}

async function readMessage(): Promise<{ tag: string; payload: Buffer }> {
  const header = await readExactly(5);
  const tag = String.fromCharCode(header.readUInt8(0));
  const length = header.readInt32BE(1);
  if (length < 4 || length > 1024 * 1024) {
    throw new Error(`invalid pgwire message length ${length}`);
  }
  return { tag, payload: await readExactly(length - 4) };
}

function startup(): Buffer {
  const params = Buffer.from("user\0opendb\0database\0opendb\0\0");
  const frame = Buffer.alloc(8 + params.length);
  frame.writeInt32BE(frame.length, 0);
  frame.writeInt32BE(196608, 4);
  params.copy(frame, 8);
  return frame;
}

function query(sql: string): Buffer {
  const payload = Buffer.from(`${sql}\0`);
  const frame = Buffer.alloc(1 + 4 + payload.length);
  frame.write("Q", 0);
  frame.writeInt32BE(payload.length + 4, 1);
  payload.copy(frame, 5);
  return frame;
}

async function waitForReady(): Promise<void> {
  for (;;) {
    const message = await readMessage();
    if (message.tag === "E") {
      throw new Error(`pgwire error: ${message.payload.toString("utf8")}`);
    }
    if (message.tag === "Z") {
      return;
    }
  }
}

async function exec(sql: string): Promise<Buffer[]> {
  client.write(query(sql));
  const rows: Buffer[] = [];
  for (;;) {
    const message = await readMessage();
    if (message.tag === "E") {
      throw new Error(`pgwire error: ${message.payload.toString("utf8")}`);
    }
    if (message.tag === "D") {
      rows.push(message.payload);
    }
    if (message.tag === "Z") {
      return rows;
    }
  }
}

async function execExpectError(sql: string): Promise<string> {
  client.write(query(sql));
  let errorPayload: string | undefined;
  for (;;) {
    const message = await readMessage();
    if (message.tag === "E") {
      errorPayload = message.payload.toString("utf8");
    }
    if (message.tag === "Z") {
      if (errorPayload === undefined) {
        throw new Error(`expected pgwire error for ${sql}`);
      }
      return errorPayload;
    }
  }
}

function rowText(row: Buffer): string[] {
  const columns = row.readUInt16BE(0);
  const values: string[] = [];
  let offset = 2;
  for (let index = 0; index < columns; index += 1) {
    const length = row.readInt32BE(offset);
    offset += 4;
    values.push(row.subarray(offset, offset + length).toString("utf8"));
    offset += length;
  }
  return values;
}

await connect();

try {
  client.write(startup());
  await waitForReady();
  await exec(`CREATE TABLE ${tableName} (id INT PRIMARY KEY, name TEXT)`);
  await exec(`INSERT INTO ${tableName} VALUES (1, 'Ada')`);
  await exec(`INSERT INTO ${tableName} VALUES (2, 'Grace')`);
  const rows = await exec(`SELECT * FROM ${tableName}`);

  if (!rows.some((row) => rowText(row).includes("Ada"))) {
    throw new Error("Ada row was not returned");
  }
  const filteredRows = await exec(`SELECT * FROM ${tableName} WHERE id = 1`);
  const filteredRow = filteredRows[0];
  if (filteredRows.length !== 1 || filteredRow === undefined || !rowText(filteredRow).includes("Ada")) {
    throw new Error(`primary-key filtered select returned unexpected rows: ${filteredRows.map(rowText).join(";")}`);
  }
  const textFilteredRows = await exec(`SELECT * FROM ${tableName} WHERE name = 'Ada'`);
  const textFilteredRow = textFilteredRows[0];
  if (textFilteredRows.length !== 1 || textFilteredRow === undefined || !rowText(textFilteredRow).includes("Ada")) {
    throw new Error(`text filtered select returned unexpected rows: ${textFilteredRows.map(rowText).join(";")}`);
  }
  const duplicateError = await execExpectError(`INSERT INTO ${tableName} VALUES (1, 'Grace')`);
  if (!duplicateError.includes("row already exists")) {
    throw new Error(`duplicate primary key was not rejected as expected: ${duplicateError}`);
  }

  // Sprint 6: extended types (BOOL, FLOAT8, TIMESTAMP), NOT NULL, DEFAULT,
  // and the named-column INSERT form must round-trip through pgwire.
  const typedTable = `typed_smoke_${tableName}`;
  await exec(
    `CREATE TABLE ${typedTable} (id INT PRIMARY KEY, label TEXT NOT NULL DEFAULT 'completed', done BOOL DEFAULT false, ratio FLOAT8, created_at TIMESTAMP NOT NULL DEFAULT NOW())`
  );
  await exec(`INSERT INTO ${typedTable} (id, ratio) VALUES (1, 0.5)`);
  await exec(`INSERT INTO ${typedTable} (id, label, done, ratio, created_at) VALUES (2, 'manual', TRUE, 1.5, 42)`);
  const typedRows = await exec(`SELECT * FROM ${typedTable}`);
  if (typedRows.length !== 2) {
    throw new Error(`typed smoke expected 2 rows, got ${typedRows.length}`);
  }
  const firstRow = typedRows[0];
  const secondRow = typedRows[1];
  if (firstRow === undefined || secondRow === undefined) {
    throw new Error("typed smoke missing rows");
  }
  const firstText = rowText(firstRow);
  const secondText = rowText(secondRow);
  if (!firstText.includes("completed") || !firstText.includes("f") || !firstText.includes("0.5")) {
    throw new Error(`typed smoke first row missing default values: ${firstText}`);
  }
  if (!secondText.includes("manual") || !secondText.includes("t") || !secondText.includes("1.5")) {
    throw new Error(`typed smoke second row missing explicit values: ${secondText}`);
  }
  const filteredTypedRows = await exec(`SELECT * FROM ${typedTable} WHERE id = 1`);
  if (filteredTypedRows.length !== 1) {
    throw new Error(`typed smoke filtered select expected 1 row, got ${filteredTypedRows.length}`);
  }

  // Sprint 7: JSONB column ingest + emit through pgwire (OID 3802).
  const jsonTable = `jsonb_smoke_${tableName}`;
  await exec(
    `CREATE TABLE ${jsonTable} (id INT PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb, meta JSONB)`
  );
  await exec(
    `INSERT INTO ${jsonTable} (id, data, meta) VALUES (1, '{"k":"v","n":7}'::jsonb, '[1,2,3]'::jsonb)`
  );
  await exec(`INSERT INTO ${jsonTable} (id) VALUES (2)`);
  const jsonRows = await exec(`SELECT * FROM ${jsonTable}`);
  if (jsonRows.length !== 2) {
    throw new Error(`jsonb smoke expected 2 rows, got ${jsonRows.length}`);
  }
  const explicitJsonRow = jsonRows[0];
  const defaultJsonRow = jsonRows[1];
  if (explicitJsonRow === undefined || defaultJsonRow === undefined) {
    throw new Error("jsonb smoke missing rows");
  }
  const explicitRowText = rowText(explicitJsonRow).join("|");
  if (!explicitRowText.includes('"k":"v"') || !explicitRowText.includes('"n":7')) {
    throw new Error(`jsonb smoke explicit row missing values: ${explicitRowText}`);
  }
  const defaultRowText = rowText(defaultJsonRow).join("|");
  if (!defaultRowText.includes("{}")) {
    throw new Error(`jsonb smoke default row missing empty object: ${defaultRowText}`);
  }

  // Sprint 8: ALTER TABLE ADD COLUMN + CREATE INDEX IF NOT EXISTS +
  // DO $$ ... EXCEPTION WHEN duplicate_object idempotence must round-trip
  // through pgwire and emit the expected command tags.
  const alterTable = `alter_smoke_${tableName}`;
  await exec(`CREATE TABLE ${alterTable} (id INT PRIMARY KEY, name TEXT)`);
  await exec(`INSERT INTO ${alterTable} (id, name) VALUES (1, 'Ada')`);
  await exec(
    `ALTER TABLE ${alterTable} ADD COLUMN status TEXT NOT NULL DEFAULT 'active'`
  );
  await exec(
    `CREATE INDEX IF NOT EXISTS ${alterTable}_name_idx ON ${alterTable} USING btree (name)`
  );
  await exec(
    `DO $$ BEGIN ALTER TABLE ${alterTable} ADD COLUMN status TEXT DEFAULT 'x'; EXCEPTION WHEN duplicate_object THEN null; END $$`
  );
  const alterRows = await exec(`SELECT * FROM ${alterTable}`);
  if (alterRows.length !== 1) {
    throw new Error(`alter smoke expected 1 row, got ${alterRows.length}`);
  }
  const alterRow = alterRows[0];
  if (alterRow === undefined) {
    throw new Error("alter smoke missing row");
  }
  const alterRowText = rowText(alterRow).join("|");
  if (!alterRowText.includes("active")) {
    throw new Error(`alter smoke missing default backfill: ${alterRowText}`);
  }

  // Sprint 9: UNIQUE / FK enforcement and DELETE through pgwire.
  const parents = `parents_smoke_${tableName}`;
  const children = `children_smoke_${tableName}`;
  await exec(`CREATE TABLE ${parents} (id INT PRIMARY KEY, name TEXT)`);
  await exec(`CREATE TABLE ${children} (id INT PRIMARY KEY, parent_id INT)`);
  await exec(
    `ALTER TABLE ${children} ADD CONSTRAINT ${children}_fk FOREIGN KEY (parent_id) REFERENCES ${parents} (id) ON DELETE CASCADE`
  );
  await exec(
    `ALTER TABLE ${parents} ADD CONSTRAINT ${parents}_unique_name UNIQUE (name)`
  );
  await exec(`INSERT INTO ${parents} (id, name) VALUES (1, 'p1')`);
  await exec(`INSERT INTO ${children} (id, parent_id) VALUES (10, 1)`);
  const duplicateUniqueError = await execExpectError(
    `INSERT INTO ${parents} (id, name) VALUES (2, 'p1')`
  );
  if (!duplicateUniqueError.includes("UNIQUE")) {
    throw new Error(`UNIQUE constraint not rejected: ${duplicateUniqueError}`);
  }
  const fkError = await execExpectError(
    `INSERT INTO ${children} (id, parent_id) VALUES (11, 99)`
  );
  if (!fkError.includes("FK")) {
    throw new Error(`FK not enforced: ${fkError}`);
  }
  await exec(`DELETE FROM ${parents} WHERE id = 1`);
  const remainingChildren = await exec(`SELECT * FROM ${children}`);
  if (remainingChildren.length !== 0) {
    throw new Error(
      `expected cascade delete to drop children, got ${remainingChildren.length} rows`
    );
  }

  // Sprint 10: ORDER BY + LIMIT + OFFSET smoke.
  const orderTable = `order_smoke_${tableName}`;
  await exec(`CREATE TABLE ${orderTable} (id INT PRIMARY KEY, label TEXT)`);
  for (let i = 1; i <= 5; i += 1) {
    await exec(`INSERT INTO ${orderTable} (id, label) VALUES (${i}, 'row-${i}')`);
  }
  const orderedRows = await exec(
    `SELECT * FROM ${orderTable} ORDER BY id DESC LIMIT 2 OFFSET 1`
  );
  if (orderedRows.length !== 2) {
    throw new Error(`order smoke expected 2 rows, got ${orderedRows.length}`);
  }
  const firstOrdered = orderedRows[0];
  if (firstOrdered === undefined) {
    throw new Error("order smoke missing first row");
  }
  const firstOrderedText = rowText(firstOrdered).join("|");
  if (!firstOrderedText.startsWith("4|")) {
    throw new Error(`order smoke expected id=4 first, got ${firstOrderedText}`);
  }

  // Sprint 10.5: INNER + LEFT JOIN through pgwire.
  const joinA = `join_a_${tableName}`;
  const joinB = `join_b_${tableName}`;
  await exec(`CREATE TABLE ${joinA} (id INT PRIMARY KEY, label TEXT)`);
  await exec(`CREATE TABLE ${joinB} (id INT PRIMARY KEY, a_id INT)`);
  await exec(`INSERT INTO ${joinA} (id, label) VALUES (1, 'a1')`);
  await exec(`INSERT INTO ${joinA} (id, label) VALUES (2, 'a2')`);
  await exec(`INSERT INTO ${joinB} (id, a_id) VALUES (10, 1)`);
  const innerRows = await exec(
    `SELECT * FROM ${joinA} INNER JOIN ${joinB} ON ${joinA}.id = ${joinB}.a_id`
  );
  if (innerRows.length !== 1) {
    throw new Error(`inner join expected 1 row, got ${innerRows.length}`);
  }
  const leftRows = await exec(
    `SELECT * FROM ${joinA} LEFT JOIN ${joinB} ON ${joinA}.id = ${joinB}.a_id ORDER BY ${joinA}.id ASC`
  );
  if (leftRows.length !== 2) {
    throw new Error(`left join expected 2 rows, got ${leftRows.length}`);
  }

  // Sprint 11: BEGIN / COMMIT / ROLLBACK no-op skeleton.
  await exec(`BEGIN`);
  await exec(`INSERT INTO ${joinA} (id, label) VALUES (3, 'a3')`);
  await exec(`COMMIT`);
  await exec(`BEGIN`);
  await exec(`ROLLBACK`);
  const finalA = await exec(`SELECT * FROM ${joinA}`);
  if (finalA.length !== 3) {
    throw new Error(`expected 3 rows after BEGIN/COMMIT, got ${finalA.length}`);
  }

  client.end();
  console.log("pgwire smoke passed");
} catch (error) {
  client.destroy();
  throw error;
}
