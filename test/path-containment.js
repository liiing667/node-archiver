import { WriteStream } from "fs";
import { assert } from "chai";
import { mkdirp } from "mkdirp";
import { JsonArchive } from "../index.js";
import { readJSON } from "./helpers/index.js";

function runArchive(build) {
  return new Promise((resolve, reject) => {
    const archive = new JsonArchive();
    const errors = [];
    const output = new WriteStream("tmp/path-containment.json");

    output.on("error", reject);
    output.on("close", function () {
      try {
        resolve({
          entries: readJSON("tmp/path-containment.json"),
          errors: errors,
        });
      } catch (err) {
        reject(err);
      }
    });

    archive.on("error", function (err) {
      errors.push(err);
    });
    archive.pipe(output);
    build(archive);
    archive.finalize().catch(reject);
  });
}

describe("archive entry path containment", function () {
  before(function () {
    mkdirp.sync("tmp");
  });

  it("rejects escaping file names before their source is read", async function () {
    const result = await runArchive(function (archive) {
      archive.file("test/fixtures/test.txt", { name: "nested/test.txt" });
      archive.file("test/fixtures/test.txt", {
        name: "nested//./child/../file.txt",
      });
      archive.file("test/fixtures/does-not-exist.txt", {
        name: "safe/../../escape.txt",
      });
      archive.file("test/fixtures/test.txt", {
        name: "C:\\safe\\..\\..\\escape.txt",
      });
      archive.file("test/fixtures/test.txt", {
        name: "safe\\..\\..\\escape.txt",
      });
    });

    assert.deepEqual(
      result.entries.map((entry) => entry.name).sort(),
      ["nested/file.txt", "nested/test.txt"],
    );
    assert.lengthOf(result.errors, 3);
    result.errors.forEach(function (err) {
      assert.equal(err.code, "ENTRYNAMEEXCEEDSPATH");
      assert.equal(err.message, "entry name exceeds path");
      assert.equal(err.data.entry, "file");
    });
  });

  it("rejects escaping glob prefixes and keeps safe prefixes", async function () {
    const result = await runArchive(function (archive) {
      archive.glob("level0.txt", { cwd: "test/fixtures/directory" }, {
        prefix: "nested",
      });
      archive.glob("level0.txt", { cwd: "test/fixtures/directory" }, {
        prefix: "/absolute",
      });
      archive.glob("level0.txt", { cwd: "test/fixtures/directory" }, {
        prefix: "safe/../../escape",
      });
      archive.glob("level0.txt", { cwd: "test/fixtures/directory" }, {
        prefix: "C:\\safe\\..\\..\\escape",
      });
    });

    assert.deepEqual(
      result.entries.map((entry) => entry.name).sort(),
      ["absolute/level0.txt", "nested/level0.txt"],
    );
    assert.lengthOf(result.errors, 2);
    result.errors.forEach(function (err) {
      assert.equal(err.code, "ENTRYNAMEEXCEEDSPATH");
      assert.equal(err.message, "entry name exceeds path");
      assert.equal(err.data.entry, "glob");
    });
  });
});
