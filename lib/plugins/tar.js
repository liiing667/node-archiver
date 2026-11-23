import zlib from "zlib";
import engine from "tar-stream";
import {
  annotateStreamError,
  collectStream,
  destroyStream,
  resolveMaxStreamSize,
} from "../utils.js";

/**
 * TAR Format Plugin
 *
 * @module plugins/tar
 * @license [MIT]{@link https://github.com/archiverjs/node-archiver/blob/master/LICENSE}
 * @copyright (c) 2012-2014 Chris Talkington, contributors.
 */
export default class Tar {
  /**
   * @constructor
   * @param {TarOptions} options
   */
  constructor(options) {
    options = this.options = {
      gzip: false,
      ...options,
    };
    if (typeof options.gzipOptions !== "object") {
      options.gzipOptions = {};
    }
    this.engine = engine.pack(options);
    this.compressor = false;
    this._collector = null;
    this._source = null;
    this._entry = null;
    if (options.gzip) {
      this.compressor = zlib.createGzip(options.gzipOptions);
      this.compressor.on("error", this._onCompressorError.bind(this));
    }
  }
  /**
   * [_onCompressorError description]
   *
   * @private
   * @param  {Error} err
   * @return void
   */
  _onCompressorError(err) {
    this.engine.emit("error", err);
  }
  /**
   * [append description]
   *
   * @param  {(Buffer|Stream)} source
   * @param  {TarEntryData} data
   * @param  {Function} callback
   * @return void
   */
  append(source, data, callback) {
    var self = this;
    data.mtime = data.date;
    function append(err, sourceBuffer) {
      if (err) {
        callback(err);
        return;
      }
      self.engine.entry(data, sourceBuffer, function (err) {
        callback(err, data);
      });
    }
    if (data.sourceType === "buffer") {
      append(null, source);
    } else if (
      data.sourceType === "stream" &&
      data.stats &&
      Number.isFinite(data.stats.size) &&
      data.stats.size >= 0
    ) {
      data.size = data.stats.size;
      self._source = source;
      var entry = (self._entry = self.engine.entry(data, function (err) {
        self._source = null;
        self._entry = null;
        callback(err, data);
      }));
      source.pipe(entry);
    } else if (data.sourceType === "stream") {
      const collectOptions = Object.assign({}, self.options, data);
      const maxSize = resolveMaxStreamSize(collectOptions);
      const identity = {
        name: data.name,
        sourcePath: data.sourcePath,
        sourceType: data.sourceType,
      };
      const collector = collectStream(
        source,
        function (err, sourceBuffer) {
          self._collector = null;
          if (err) {
            annotateStreamError(err, identity, "collect");
          }
          append(err, sourceBuffer);
        },
        maxSize,
      );
      self._collector = collector.active ? collector : null;
    }
  }
  /**
   * Cancel any in-flight stream collection and release its buffers.
   *
   * @return void
   */
  abort() {
    if (this._collector) {
      const collector = this._collector;
      this._collector = null;
      collector.cancel();
    }
    if (this._entry && this._source) {
      try {
        this._source.unpipe(this._entry);
      } catch (err) {
        // The entry may already have been destroyed by the engine.
      }
    }
    if (this._source) {
      destroyStream(this._source);
      this._source = null;
    }
    this._entry = null;
  }
  /**
   * [finalize description]
   *
   * @return void
   */
  finalize() {
    this.engine.finalize();
  }
  /**
   * [on description]
   *
   * @return this.engine
   */
  on() {
    return this.engine.on.apply(this.engine, arguments);
  }
  /**
   * [removeListener description]
   *
   * @return this.engine
   */
  removeListener() {
    return this.engine.removeListener.apply(this.engine, arguments);
  }
  /**
   * [pipe description]
   *
   * @param  {String} destination
   * @param  {Object} options
   * @return this.engine
   */
  pipe(destination, options) {
    if (this.compressor) {
      return this.engine.pipe
        .apply(this.engine, [this.compressor])
        .pipe(destination, options);
    } else {
      return this.engine.pipe.apply(this.engine, arguments);
    }
  }
  /**
   * [unpipe description]
   *
   * @return this.engine
   */
  unpipe() {
    if (this.compressor) {
      return this.compressor.unpipe.apply(this.compressor, arguments);
    } else {
      return this.engine.unpipe.apply(this.engine, arguments);
    }
  }
}
