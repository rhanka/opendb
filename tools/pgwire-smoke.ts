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
  const rows = await exec(`SELECT * FROM ${tableName}`);

  if (!rows.some((row) => rowText(row).includes("Ada"))) {
    throw new Error("Ada row was not returned");
  }
  const duplicateError = await execExpectError(`INSERT INTO ${tableName} VALUES (1, 'Grace')`);
  if (!duplicateError.includes("row already exists")) {
    throw new Error(`duplicate primary key was not rejected as expected: ${duplicateError}`);
  }

  client.end();
  console.log("pgwire smoke passed");
} catch (error) {
  client.destroy();
  throw error;
}
