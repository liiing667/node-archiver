import normalizePath from "normalize-path";
import { PassThrough } from "readable-stream";
import { isStream } from "is-stream";

export function collectStream(source, callback) {
  var collection = [];
  var size = 0;

  source.on("error", callback);

  source.on("data", function (chunk) {
    collection.push(chunk);
    size += chunk.length;
  });

  source.on("end", function () {
    var buf = Buffer.alloc(size);
    var offset = 0;

    collection.forEach(function (data) {
      data.copy(buf, offset);
      offset += data.length;
    });

    callback(null, buf);
  });
}

export function dateify(dateish) {
  dateish = dateish || new Date();

  if (dateish instanceof Date) {
    dateish = dateish;
  } else if (typeof dateish === "string") {
    dateish = new Date(dateish);
  } else {
    dateish = new Date();
  }

  return dateish;
}

export function normalizeInputSource(source) {
  if (source === null) {
    return Buffer.alloc(0);
  } else if (typeof source === "string") {
    return Buffer.from(source);
  } else if (isStream(source)) {
    // Always pipe through a PassThrough stream to guarantee pausing the stream if it's already flowing,
    // since it will only be processed in a (distant) future iteration of the event loop, and will lose
    // data if already flowing now.
    return source.pipe(new PassThrough());
  }

  return source;
}

const DRIVE_PATH = /^[A-Za-z]:/;

/**
 * Normalizes a path that is meant to become an archive entry name.
 *
 * The result is relative to the archive root and uses forward slashes. Dot
 * segments are resolved. Options control whether an absolute path or a leading
 * parent-directory segment may be stripped (legacy source-path behaviour) or
 * must be reported as an escaping name.
 *
 * Returns `null` when the name would leave the archive root.
 *
 * @param  {String} filepath
 * @param  {Object} [options]
 * @param  {Boolean} [options.allowAbsolute=false]
 * @param  {Boolean} [options.stripLeadingTraversal=false]
 * @param  {Boolean} [options.allowAboveRoot=false]
 * @param  {Boolean} [options.clampAboveRoot=false]
 * @return {(String|null)}
 */
export function normalizeEntryPath(filepath, options = {}) {
  if (typeof filepath !== "string") {
    return null;
  }

  const {
    allowAbsolute = false,
    stripLeadingTraversal = false,
    allowAboveRoot = false,
    clampAboveRoot = false,
  } = options;

  let normalized = normalizePath(filepath, false);
  const trailingSlash = normalized.length > 1 && normalized.slice(-1) === "/";
  const absolute = DRIVE_PATH.test(normalized) || normalized.startsWith("/");

  if (absolute) {
    if (!allowAbsolute) {
      return null;
    }
    normalized = normalized.replace(DRIVE_PATH, "").replace(/^\/+/, "");
  }

  if (stripLeadingTraversal) {
    while (normalized === ".." || normalized.startsWith("../")) {
      normalized = normalized === ".." ? "" : normalized.slice(3);
      normalized = normalized.replace(/^\/+/, "");
    }
  }

  const segments = [];
  for (const segment of normalized.split("/")) {
    if (segment === "" || segment === ".") {
      continue;
    }
    if (segment === "..") {
      if (segments.length > 0 && segments[segments.length - 1] !== "..") {
        segments.pop();
      } else if (allowAboveRoot) {
        segments.push("..");
      } else if (!clampAboveRoot) {
        return null;
      }
    } else {
      segments.push(segment);
    }
  }

  let result = segments.join("/");
  if (trailingSlash && result && result.slice(-1) !== "/") {
    result += "/";
  }
  return result;
}

export function sanitizePath(filepath) {
  const normalized = normalizePath(filepath, false)
    .replace(/^\w+:/, "")
    .replace(/^(\.\.\/|\/)+/, "");

  return (
    normalizeEntryPath(normalized, {
      stripLeadingTraversal: true,
      clampAboveRoot: true,
    }) || ""
  );
}

export function trailingSlashIt(str) {
  return str.slice(-1) !== "/" ? str + "/" : str;
}
