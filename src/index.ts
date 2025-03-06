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
  quiet?: boolean;
};

export type RemoveOptions = {
  quiet?: boolean;
  ifVersion?: number;
};

interface DefaultSerializer<V> {
  encoder: { encode: (value: V) => Buffer };
  decoder: { encode: (value: Buffer) => V };
}

// This is the original LMDBx DB type plus our serializer.
type DB<V = any, K extends Key = Key> = Database<V, K> & DefaultSerializer<V>;

// Define our wrapped DB interface: we omit the original put/remove and add our simplified versions.
export interface WrappedDB<V = any, K extends Key = Key>
  extends Omit<Database<V, K>, "put" | "remove">,
    DefaultSerializer<V> {
  put(id: K, value: V, options?: PutOptions): Promise<boolean>;
  remove(id: K, options?: RemoveOptions): Promise<boolean>;
}

export class Store<V = any, K extends Key = Key> extends EventEmitter {
  protected env: RootDatabase;
  // Open the TTL bucket directly so it's not wrapped.
  protected ttlBucket: Database<string, string>;
  protected dbs: Map<string, WrappedDB<any, K>>;
  protected flushing: boolean = false;

  constructor(name: string, options: RootDatabaseOptions) {
    super();
    this.dbs = new Map();
    this.env = open(name, options);
    // Open TTL bucket directly.
    this.ttlBucket = this.env.openDB("ttl", { cache: true });
  }

  // Build a TTL key in the format "exp:bucket:key"
  protected ttlKey(exp: number, bucket: string, key: string): string {
    return `${exp}:${bucket}:${key}`;
  }

  /**
   * Wrap a given DB instance using a levelup-style approach.
   * All methods and properties are available via the prototype,
   * but we override put and remove with our custom implementations.
   */
  protected wrapDB<TV>(db: DB<TV, K>, bucketName: string): WrappedDB<TV, K> {
    const self = this;
    // Start with a Partial of our WrappedDB, using Object.create to inherit original methods.
    const wrapped: Partial<WrappedDB<TV, K>> = Object.create(db);

    // Override put with our custom logic and proper type annotation.
    wrapped.put = function (
      id: K,
      value: TV,
      options?: PutOptions
    ): Promise<boolean> {
      let result: Promise<boolean>;
      if (
        options &&
        options.version !== undefined &&
        options.ifVersion !== undefined
      ) {
        result = db.put(id, value, options.version, options.ifVersion);
      } else if (options && options.version !== undefined) {
        result = db.put(id, value, options.version);
      } else {
        result = db.put(id, value);
      }

      if (options?.ttl) {
        const exp = Date.now() + options.ttl;
        const ttlEntryKey = self.ttlKey(exp, bucketName, String(id));
        self.ttlBucket.put(ttlEntryKey, "");
      }

      if (!options?.quiet) {
        self.emit("change", {
          op: "put",
          bucket: bucketName,
          id,
          value: db.encoder.encode(value),
          version: options?.version,
          ttl: options?.ttl,
        });
      }
      return result;
    };

    // Override remove with our custom logic and proper type annotation.
    wrapped.remove = function (
      id: K,
      options?: RemoveOptions
    ): Promise<boolean> {
      if (!options?.quiet) {
        const current = db.get(id);
        self.emit("change", {
          op: "remove",
          bucket: bucketName,
          id,
          value: current,
          version: options?.ifVersion,
        });
      }
      if (options?.ifVersion !== undefined) {
        return db.remove(id, options.ifVersion);
      }
      return db.remove(id);
    };

    // Return the wrapped object cast to WrappedDB.
    return wrapped as WrappedDB<TV, K>;
  }

  /**
   * Retrieve or create a sub-database.
   * The returned instance is wrapped so that our custom put/remove
   * (with TTL and change event functionality) are available.
   */
  bucket<TV = any>(name: string, options?: DatabaseOptions): WrappedDB<TV, K> {
    let db = this.dbs.get(name);
    if (!db) {
      const opts: DatabaseOptions = { cache: true, ...options };
      const raw = this.env.openDB<TV, K>(name, opts) as DB<TV, K>;
      db = this.wrapDB(raw, name);
      this.dbs.set(name, db);
    }
    return db;
  }

  /**
   * Clean up expired TTL entries in batch.
   */
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
