import { EventEmitter } from "events";
import {
  Database,
  DatabaseOptions,
  Key,
  open,
  RootDatabase,
  RootDatabaseOptions,
} from "lmdbx";

export type PutOptions = {
  version?: number;
  ttl?: number; // TTL in milliseconds
  ifVersion?: number;
};

export type RemoveOptions = {
  quiet?: boolean;
  ifVersion?: number;
};

interface DefaultSerializer<V> {
  encoder: { encode: (value: V) => Buffer };
  decoder: { encode: (value: V) => Buffer };
}

type DB<V = any, K extends Key = Key> = Database<V, K> & DefaultSerializer<V>;

export class Store<V = any, K extends Key = Key> extends EventEmitter {
  protected env: RootDatabase;
  // Open the TTL bucket directly so it's not patched (and no events are emitted).
  protected ttlBucket: Database<string, string>;
  protected dbs: Map<string, DB<any, K>>;
  protected flushing: boolean = false;

  constructor(name: string, options: RootDatabaseOptions) {
    super();
    this.dbs = new Map();
    this.env = open(name, options);
    // Open TTL bucket directly (bypassing our patching logic).
    this.ttlBucket = this.env.openDB("ttl", { cache: true });
  }

  // Build a TTL key in the format "exp:bucket:key"
  protected ttlKey(exp: number, bucket: string, key: string): string {
    return `${exp}:${bucket}:${key}`;
  }

  // Patch a given database to wrap its put and remove methods.
  protected _patch(db: DB, bucketName: string) {
    const origPut = db.put.bind(db);
    const origRemove = db.remove.bind(db);
    const self = this;

    db.put = (
      id: K,
      value: V,
      verOrOpts?: number | PutOptions,
      ifVersion?: number
    ) => {
      let options: PutOptions = {};
      if (typeof verOrOpts === "number") {
        options.version = verOrOpts;
      } else if (typeof verOrOpts === "object" && verOrOpts !== null) {
        options = verOrOpts;
      }

      const result = origPut(id, value, options.version as number, ifVersion);

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
        ttl: options.ttl,
      });

      return result;
    };

    db.remove = async (
      id: K,
      opts?: number | RemoveOptions
    ): Promise<boolean> => {
      let quiet = false;
      let version: number | undefined;

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
          version,
        });
      }

      if (typeof opts === "object" && opts !== null && opts.quiet) {
        return origRemove(id, version);
      }

      return origRemove(id, opts as any);
    };
  }

  // Retrieve or create a sub-database.
  bucket<TV = any>(name: string, options?: DatabaseOptions): DB<TV, K> {
    let db = this.dbs.get(name);

    if (!db) {
      const opts: DatabaseOptions = { cache: true, ...options };
      db = this.env.openDB<TV, K>(name, opts) as DB<TV, K>;
      this.dbs.set(name, db);
      this._patch(db, name);
    }

    return db;
  }

  // Clean up expired TTL entries in batch.
  async clean() {
    if (this.flushing) return;
    this.flushing = true;

    const keysToDelete: Array<{
      ttlKey: string;
      bucketName: string;
      id: string;
    }> = [];

    const now = Date.now().toString();

    for (const key of this.ttlBucket.getKeys({ end: now })) {
      const parts = key.split(":");
      if (parts.length < 3) continue;
      const bucketName = parts[1];
      const id = parts.slice(2).join(":");
      keysToDelete.push({ ttlKey: key, bucketName, id });
    }

    // Batch removal using a transaction
    await this.env.transaction(() => {
      for (const { ttlKey, bucketName, id } of keysToDelete) {
        const bucket = this.bucket(bucketName);
        bucket.remove(id as K, { quiet: true });
        this.ttlBucket.remove(ttlKey);
      }
    });

    this.flushing = false;
  }
}
