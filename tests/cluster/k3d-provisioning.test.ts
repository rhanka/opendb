import { describe, it, expect } from "vitest";
import { k3dBinaryAssetName, parseK3dUpOptions } from "../../tools/k3d-up.js";
import { parseK3dDownOptions } from "../../tools/k3d-down.js";

describe("k3d-up option parsing", () => {
  it("defaults cluster name to opendb-dev", () => {
    const previous = process.env.OPENDB_K3D_CLUSTER;
    delete process.env.OPENDB_K3D_CLUSTER;
    try {
      const options = parseK3dUpOptions([]);
      expect(options.clusterName).toBe("opendb-dev");
      expect(options.skipBuild).toBe(false);
      expect(options.apiPort).toBeUndefined();
    } finally {
      if (previous !== undefined) {
        process.env.OPENDB_K3D_CLUSTER = previous;
      }
    }
  });

  it("accepts --cluster-name and --api-port", () => {
    const options = parseK3dUpOptions(["--cluster-name", "opendb-ci", "--api-port", "6550"]);
    expect(options.clusterName).toBe("opendb-ci");
    expect(options.apiPort).toBe(6550);
  });

  it("accepts --skip-build", () => {
    const options = parseK3dUpOptions(["--skip-build"]);
    expect(options.skipBuild).toBe(true);
  });

  it("rejects unknown arguments", () => {
    expect(() => parseK3dUpOptions(["--bogus"])).toThrow("unknown argument: --bogus");
  });

  it("OPENDB_K3D_SKIP_BUILD=1 enables skipBuild", () => {
    const previous = process.env.OPENDB_K3D_SKIP_BUILD;
    process.env.OPENDB_K3D_SKIP_BUILD = "1";
    try {
      const options = parseK3dUpOptions([]);
      expect(options.skipBuild).toBe(true);
    } finally {
      if (previous === undefined) {
        delete process.env.OPENDB_K3D_SKIP_BUILD;
      } else {
        process.env.OPENDB_K3D_SKIP_BUILD = previous;
      }
    }
  });
});

describe("k3d-down option parsing", () => {
  it("defaults cluster name to opendb-dev", () => {
    const previous = process.env.OPENDB_K3D_CLUSTER;
    delete process.env.OPENDB_K3D_CLUSTER;
    try {
      const options = parseK3dDownOptions([]);
      expect(options.clusterName).toBe("opendb-dev");
    } finally {
      if (previous !== undefined) {
        process.env.OPENDB_K3D_CLUSTER = previous;
      }
    }
  });

  it("accepts --cluster-name override", () => {
    const options = parseK3dDownOptions(["--cluster-name", "opendb-ci"]);
    expect(options.clusterName).toBe("opendb-ci");
  });
});

describe("k3d binary asset name", () => {
  it("maps linux x64 to k3d-linux-amd64", () => {
    expect(k3dBinaryAssetName("linux", "x64")).toBe("k3d-linux-amd64");
  });

  it("maps darwin arm64 to k3d-darwin-arm64", () => {
    expect(k3dBinaryAssetName("darwin", "arm64")).toBe("k3d-darwin-arm64");
  });

  it("adds .exe suffix on windows", () => {
    expect(k3dBinaryAssetName("win32", "x64")).toBe("k3d-windows-amd64.exe");
  });
});
