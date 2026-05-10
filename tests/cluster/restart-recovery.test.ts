import { describe, it, expect } from "vitest";
import { parseSmokeOptions, recoveryConditionIsTrue } from "../../tools/k3s-smoke.js";

describe("restart recovery flag", () => {
  it("parses --with-restart-recovery", () => {
    const options = parseSmokeOptions(["--with-restart-recovery"]);
    expect(options.withRestartRecovery).toBe(true);
  });

  it("defaults --with-restart-recovery to false", () => {
    const options = parseSmokeOptions([]);
    expect(options.withRestartRecovery).toBe(false);
  });

  it("OPENDB_K3S_WITH_RESTART_RECOVERY=1 enables the flag", () => {
    const previous = process.env.OPENDB_K3S_WITH_RESTART_RECOVERY;
    process.env.OPENDB_K3S_WITH_RESTART_RECOVERY = "1";
    try {
      const options = parseSmokeOptions([]);
      expect(options.withRestartRecovery).toBe(true);
    } finally {
      if (previous === undefined) {
        delete process.env.OPENDB_K3S_WITH_RESTART_RECOVERY;
      } else {
        process.env.OPENDB_K3S_WITH_RESTART_RECOVERY = previous;
      }
    }
  });
});

describe("recoveryConditionIsTrue", () => {
  it("returns true when status.conditions[type=Recovered].status=True", () => {
    expect(
      recoveryConditionIsTrue({
        status: {
          conditions: [
            { type: "Ready", status: "True" },
            { type: "Recovered", status: "True" }
          ]
        }
      })
    ).toBe(true);
  });

  it("returns false when Recovered condition is Unknown", () => {
    expect(
      recoveryConditionIsTrue({
        status: {
          conditions: [
            { type: "Ready", status: "True" },
            { type: "Recovered", status: "Unknown" }
          ]
        }
      })
    ).toBe(false);
  });

  it("returns false when Recovered condition is missing", () => {
    expect(
      recoveryConditionIsTrue({
        status: {
          conditions: [{ type: "Ready", status: "True" }]
        }
      })
    ).toBe(false);
  });

  it("returns false on malformed input", () => {
    expect(recoveryConditionIsTrue(null)).toBe(false);
    expect(recoveryConditionIsTrue({})).toBe(false);
    expect(recoveryConditionIsTrue({ status: null })).toBe(false);
    expect(recoveryConditionIsTrue({ status: { conditions: "not-an-array" } })).toBe(false);
  });
});
