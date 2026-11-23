import { Transform } from "readable-stream";
import crc32 from "buffer-crc32";
import {
  annotateStreamError,
  collectStream,
  resolveMaxStreamSize,
} from "../utils.js";

/**
 * JSON Format Plugin
 *
 * @module plugins/json
 * @license [MIT]{@link https://github.com/archiverjs/node-archiver/blob/master/LICENSE}
 * @copyright (c) 2012-2014 Chris Talkington, contributors.
 */
export default class Json extends Transform {
  /**
   * @constructor
   * @param {(JsonOptions|TransformOptions)} options
   */
  constructor(options) {
    super({ ...options });
    this.options = options || {};
    this.files = [];
    this._collector = null;
  }
  /**
   * [_transform description]
   *
   * @private
   * @param  {Buffer}   chunk
   * @param  {String}   encoding
   * @param  {Function} callback
   * @return void
   */
  _transform(chunk, encoding, callback) {
    callback(null, chunk);
  }
  /**
   * [_writeStringified description]
   *
   * @private
   * @return void
   */
  _writeStringified() {
    var fileString = JSON.stringify(this.files);
    this.write(fileString);
  }
  /**
   * [append description]
   *
   * @param  {(Buffer|Stream)}   source
   * @param  {EntryData}   data
   * @param  {Function} callback
   * @return void
   */
  append(source, data, callback) {
    var self = this;
    data.crc32 = 0;
    function onend(err, sourceBuffer) {
      if (err) {
        callback(err);
        return;
      }
      data.size = sourceBuffer.length || 0;
      data.crc32 = crc32.unsigned(sourceBuffer);
      self.files.push(data);
      callback(null, data);
    }
    if (data.sourceType === "buffer") {
      onend(null, source);
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
          onend(err, sourceBuffer);
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
  }
  /**
   * [finalize description]
   *
   * @return void
   */
  finalize() {
    this._writeStringified();
    this.end();
  }
}
