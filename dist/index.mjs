// src/index.ts
import { EventEmitter } from "events";
import {
  open
} from "lmdbx";
var Store = class extends EventEmitter {
  constructor(name, options) {
    super();
    this.flushing = false;
    this.dbs = /* @__PURE__ */ new Map();
    this.env = open(name, options);
    this.ttlBucket = this.env.openDB("ttl", { cache: true });
  }
  // Build a TTL key in the format "exp:bucket:key"
  ttlKey(exp, bucket, key) {
    return `${exp}:${bucket}:${key}`;
  }
  // Patch a given database to wrap its put and remove methods.
  _patch(db, bucketName) {
    const origPut = db.put.bind(db);
    const origRemove = db.remove.bind(db);
    const self = this;
    db.put = (id, value, verOrOpts, ifVersion) => {
      let options = {};
      if (typeof verOrOpts === "number") {
        options.version = verOrOpts;
      } else if (typeof verOrOpts === "object" && verOrOpts !== null) {
        options = verOrOpts;
      }
      const result = origPut(id, value, options.version, ifVersion);
      if (options.ttl) {
        const exp = Date.now() + options.ttl;
        const ttlEntryKey = self.ttlKey(exp, bucketName, String(id));
        self.ttlBucket.put(ttlEntryKey, "");
      }
      self.emit("change", {
        op: "put",
        bucket: bucketName,
        id,
        value: db.encoder.encode(value),
        version: options.version,
        ttl: options.ttl
      });
      return result;
    };
    db.remove = async (id, opts) => {
      let quiet = false;
      let version;
      if (typeof opts === "number") {
        version = opts;
      } else if (typeof opts === "object" && opts !== null) {
        quiet = !!opts.quiet;
        version = opts.ifVersion;
      }
      if (!quiet) {
        const current = db.get(id);
        self.emit("change", {
          op: "remove",
          bucket: bucketName,
          id,
          value: current,
          version
        });
      }
      if (typeof opts === "object" && opts !== null && opts.quiet) {
        return origRemove(id, version);
      }
      return origRemove(id, opts);
    };
  }
  // Retrieve or create a sub-database.
  bucket(name, options) {
    let db = this.dbs.get(name);
    if (!db) {
      const opts = { cache: true, ...options };
      db = this.env.openDB(name, opts);
      this.dbs.set(name, db);
      this._patch(db, name);
    }
    return db;
  }
  // Clean up expired TTL entries in batch.
  async clean() {
    if (this.flushing) return;
    this.flushing = true;
    const keysToDelete = [];
    const now = Date.now().toString();
    for (const key of this.ttlBucket.getKeys({ end: now })) {
      const parts = key.split(":");
      if (parts.length < 3) continue;
      const bucketName = parts[1];
      const id = parts.slice(2).join(":");
      keysToDelete.push({ ttlKey: key, bucketName, id });
    }
    await this.env.transaction(() => {
      for (const { ttlKey, bucketName, id } of keysToDelete) {
        const bucket = this.bucket(bucketName);
        bucket.remove(id, { quiet: true });
        this.ttlBucket.remove(ttlKey);
      }
    });
    this.flushing = false;
  }
};
export {
  Store
};
//# sourceMappingURL=index.mjs.map