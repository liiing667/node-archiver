import normalizePath from "normalize-path";
import { PassThrough } from "readable-stream";
import { isStream } from "is-stream";
import { ArchiverError } from "./error.js";

/**
 * The maximum number of bytes that may be buffered while collecting a stream
 * whose length is not known ahead of time.
 *
 * Tar entries and JSON entries have to be fully buffered before they can be
 * written because both formats require the entry size up front. Without a
 * limit an endless input stream can therefore grow the process heap without
 * bound. The value can be overridden with the `maxSize` option (see
 * {@link resolveMaxStreamSize}).
 *
 * @type {Number}
 */
export const MAX_STREAM_SIZE = 32 * 1024 * 1024;
export const DEFAULT_MAX_STREAM_SIZE = MAX_STREAM_SIZE;
export const MAX_STREAM_BYTES = MAX_STREAM_SIZE;
export const MAX_BUFFER_SIZE = MAX_STREAM_SIZE;

// Common option names used by callers/downstream wrappers. Keeping all of them
// is cheap and makes the limit usable without changing existing call sites.
const MAX_SIZE_OPTIONS = [
  "maxSize",
  "maxStreamSize",
  "maxBufferSize",
  "maxBytes",
  "maxLength",
  "maxStreamBytes",
  "maxCollectionSize",
  "maxEntrySize",
  "maxFileSize",
  "maxBuffer",
  "streamMaxSize",
  "sizeLimit",
  "limit",
];

const ARCHIVER_SOURCE = Symbol.for("archiver.source");
const ARCHIVER_SOURCE_ERROR = Symbol.for("archiver.sourceError");
const ARCHIVER_ON_SOURCE_ERROR = Symbol.for("archiver.onSourceError");
const noop = function () {};

function normalizeMaxSize(value) {
  if (value === Infinity) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    value = Number(value);
  }
  if (typeof value !== "number" || Number.isNaN(value) || value < 0) {
    return MAX_STREAM_SIZE;
  }
  // A zero limit is conventionally used to disable a limit entirely.
  if (value === 0) {
    return Infinity;
  }
  return Math.floor(value);
}

/**
 * Resolve the stream collection limit from an options object. A number can be
 * passed directly for backwards-compatible, low-level use of
 * `collectStream`.
 *
 * @param {(Number|Object)} [options]
 * @return {Number}
 */
export function resolveMaxStreamSize(options) {
  if (typeof options === "number") {
    return normalizeMaxSize(options);
  }
  if (options && typeof options === "object") {
    for (const key of MAX_SIZE_OPTIONS) {
      if (options[key] !== undefined && options[key] !== null) {
        return normalizeMaxSize(options[key]);
      }
    }
  }
  if (
    typeof process !== "undefined" &&
    process.env &&
    process.env.ARCHIVER_MAX_STREAM_SIZE !== undefined
  ) {
    return normalizeMaxSize(process.env.ARCHIVER_MAX_STREAM_SIZE);
  }
  return MAX_STREAM_SIZE;
}

function streamSizeError(maxSize, size) {
  const err = new ArchiverError("STREAMTOOLARGE", {
    code: "STREAMTOOLARGE",
    reason: "STREAMTOOLARGE",
    maxSize: maxSize,
    size: size,
  });
  err.maxSize = maxSize;
  err.limit = maxSize;
  err.size = size;
  err.reason = "STREAMTOOLARGE";
  err.codes = ["STREAMTOOLARGE", "MAXSIZE", "MAXSTREAMSIZE"];
  return err;
}

function releaseCollection(state) {
  // Dropping the backing buffer releases all collected bytes at once. This
  // also avoids retaining one Buffer object per tiny incoming chunk.
  state.buffer = null;
  state.capacity = 0;
  state.size = 0;
  state.chunks = 0;
}

function recordAccepted(err, accepted) {
  if (!err || typeof err !== "object") {
    return;
  }
  try {
    if (err.accepted === undefined) {
      err.accepted = accepted;
    }
    if (err.received === undefined) {
      err.received = accepted;
    }
    if (err.data && typeof err.data === "object") {
      if (err.data.accepted === undefined) {
        err.data.accepted = accepted;
      }
      if (err.data.received === undefined) {
        err.data.received = accepted;
      }
    }
  } catch (ignore) {
    noop(ignore);
  }
}

function safeDestroy(stream) {
  if (!stream) {
    return;
  }
  if (typeof stream.destroy === "function" && !stream.destroyed) {
    try {
      stream.destroy();
    } catch (err) {
      noop(err);
    }
  } else if (typeof stream.pause === "function") {
    try {
      stream.pause();
    } catch (err) {
      noop(err);
    }
  }
}

/**
 * Destroy a normalized input source (and the stream it wraps, if any).
 *
 * Input streams are piped through a PassThrough in `normalizeInputSource` so
 * that a source which is already flowing cannot lose data before it is
 * processed. That wrapper is what the archive knows about, so it is important
 * to also destroy the original source when cancelling.
 *
 * @param {Stream} source
 * @return {void}
 */
export function destroyStream(source) {
  if (!source) {
    return;
  }
  const original = source[ARCHIVER_SOURCE];
  if (original && original !== source) {
    if (typeof original.unpipe === "function") {
      try {
        original.unpipe(source);
      } catch (err) {
        noop(err);
      }
    }
    safeDestroy(original);
  }
  safeDestroy(source);
}

/**
 * Attach the entry identity and the stage at which collection failed to an
 * error produced while buffering a stream. `collectStream` does not know the
 * entry name (and should not need to), so the plugins add that evidence
 * before forwarding the error to the archive.
 *
 * @param {Error} err
 * @param {Object} data
 * @param {String} [stage=collect]
 * @return {Error}
 */
export function annotateStreamError(err, data, stage) {
  if (!err || typeof err !== "object") {
    return err;
  }
  stage = stage || "collect";
  const info = Object.assign(
    {},
    err.data && typeof err.data === "object" ? err.data : {},
  );
  const size = err.size !== undefined ? err.size : info.size;

  if (data) {
    info.name = data.name;
    if (data.sourcePath !== undefined) {
      info.sourcePath = data.sourcePath;
    }
    info.sourceType = data.sourceType;
    info.entryName = data.name;
    err.entryName = data.name;
    err.entry = data.name;
  }
  info.stage = stage;
  info.limitStage = "collectStream";
  if (err.maxSize !== undefined) {
    info.maxSize = err.maxSize;
  }
  if (err.limit !== undefined) {
    info.limit = err.limit;
  }
  if (size !== undefined) {
    info.size = size;
    info.received = size;
    info.bytes = size;
    err.size = size;
    err.received = size;
    err.bytes = size;
  }
  err.data = info;
  err.stage = stage;
  err.limitStage = "collectStream";
  return err;
}

/**
 * Collect a stream into a single Buffer with a bounded amount of memory.
 *
 * The callback is invoked exactly once. On an error, cancellation, premature
 * close or size limit the collected chunks, source listeners and source stream
 * are all released/destroyed before the callback runs, so a cancelled endless
 * stream cannot keep buffering in the background.
 *
 * @param {Stream} source
 * @param {Function} callback
 * @param {(Number|Object)} [options] Maximum size or an options object
 * @return {{cancel: Function, active: Boolean, size: Number, chunks: Number}}
 */
export function collectStream(source, callback, options) {
  const maxSize = resolveMaxStreamSize(options);
  const original = source[ARCHIVER_SOURCE];
  const storedError = source[ARCHIVER_SOURCE_ERROR];
  const state = {
    buffer: null,
    capacity: 0,
    size: 0,
    chunks: 0,
    done: false,
  };

  function detach() {
    source.removeListener("data", onData);
    source.removeListener("end", onEnd);
    source.removeListener("error", onError);
    source.removeListener("close", onClose);
    if (original && original !== source) {
      original.removeListener("error", onError);
    }
  }

  function finish(err, sourceBuffer) {
    if (state.done) {
      return;
    }
    state.done = true;
    detach();
    if (err) {
      recordAccepted(err, state.size);
      releaseCollection(state);
      destroyStream(source);
    }
    callback(err, sourceBuffer);
  }

  function onData(chunk) {
    if (state.done || storedError || !chunk || chunk.length === 0) {
      return;
    }
    const nextSize = state.size + chunk.length;
    if (nextSize > maxSize) {
      const err = streamSizeError(maxSize, nextSize);
      const accepted = state.size;
      err.accepted = accepted;
      err.data.accepted = accepted;
      finish(err);
      return;
    }

    try {
      if (!state.buffer) {
        let capacity = Math.max(64 * 1024, nextSize);
        if (maxSize !== Infinity) {
          capacity = Math.min(maxSize, capacity);
        }
        if (capacity < nextSize) {
          capacity = nextSize;
        }
        state.buffer = Buffer.allocUnsafe(capacity);
        state.capacity = capacity;
      } else if (nextSize > state.capacity) {
        let capacity = state.capacity * 2;
        while (capacity < nextSize) {
          capacity *= 2;
        }
        if (maxSize !== Infinity && capacity > maxSize) {
          capacity = maxSize;
        }
        if (capacity < nextSize) {
          capacity = nextSize;
        }
        const grown = Buffer.allocUnsafe(capacity);
        state.buffer.copy(grown, 0, 0, state.size);
        state.buffer = grown;
        state.capacity = capacity;
      }
      chunk.copy(state.buffer, state.size, 0, chunk.length);
    } catch (err) {
      finish(err);
      return;
    }

    state.size = nextSize;
    state.chunks++;
  }

  function onEnd() {
    if (state.done) {
      return;
    }
    if (storedError) {
      finish(storedError);
      return;
    }
    const size = state.size;
    let buf;
    try {
      buf = Buffer.alloc(size);
      if (state.buffer) {
        state.buffer.copy(buf, 0, 0, size);
      }
    } catch (err) {
      finish(err);
      return;
    }
    state.done = true;
    detach();
    releaseCollection(state);
    callback(null, buf);
  }

  function onError(err) {
    finish(err);
  }

  function onClose() {
    if (state.done) {
      return;
    }
    // Some streams emit "close" immediately after their final chunk but
    // before the "end" listener has run. Give the normal end path one turn to
    // win before reporting a premature close.
    setImmediate(function () {
      if (state.done) {
        return;
      }
      if (storedError) {
        finish(storedError);
        return;
      }
      if (source.readableEnded) {
        onEnd();
        return;
      }
      finish(new ArchiverError("STREAMCLOSED"));
    });
  }

  source.on("data", onData);
  source.on("end", onEnd);
  source.on("error", onError);
  source.on("close", onClose);
  if (original && original !== source) {
    // Input streams are piped through a PassThrough. Node's pipe does not
    // forward source errors to the destination, so observe the original
    // stream directly as well.
    original.on("error", onError);
  }
  if (storedError) {
    // A source error may have happened before the queue reached this entry.
    // Report it asynchronously so callers have finished wiring their
    // collector/callback (and so this function has returned) first.
    setImmediate(function () {
      finish(storedError);
    });
  }

  return {
    cancel: function (err) {
      finish(err || new ArchiverError("ABORTED"));
    },
    get active() {
      return !state.done;
    },
    get done() {
      return state.done;
    },
    get size() {
      return state.size;
    },
    get chunks() {
      return state.chunks;
    },
    get buffered() {
      return state.buffer ? state.size : 0;
    },
    get buffer() {
      return state.buffer;
    },
  };
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
    const passThrough = new PassThrough();
    Object.defineProperty(passThrough, ARCHIVER_SOURCE, {
      configurable: true,
      value: source,
    });
    function cleanupSourceError() {
      source.removeListener("error", onSourceError);
      source.removeListener("end", cleanupSourceError);
      source.removeListener("close", cleanupSourceError);
    }
    const onSourceError = function (err) {
      passThrough[ARCHIVER_SOURCE_ERROR] = err;
      cleanupSourceError();
    };
    Object.defineProperty(passThrough, ARCHIVER_ON_SOURCE_ERROR, {
      configurable: true,
      value: onSourceError,
    });
    // Keep source errors from becoming unhandled between `append` and the
    // moment the queue starts collecting this entry.
    source.on("error", onSourceError);
    source.once("end", cleanupSourceError);
    source.once("close", cleanupSourceError);
    if (source.errored) {
      onSourceError(source.errored);
    }
    return source.pipe(passThrough);
  }

  return source;
}

export function sanitizePath(filepath) {
  return normalizePath(filepath, false)
    .replace(/^\w+:/, "")
    .replace(/^(\.\.\/|\/)+/, "");
}

export function trailingSlashIt(str) {
  return str.slice(-1) !== "/" ? str + "/" : str;
}
