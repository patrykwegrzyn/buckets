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
  ttl?: number;
  ifVersion?: number;
  quiet?: boolean;
};

export type RemoveOptions = {
  quiet?: boolean;
  ifVersion?: number;
};

interface DefaultSerializer<V> {
  encoder: { encode: (value: V) => Buffer | string };
  decoder: { decode: (value: Buffer | string) => V };
  encoding: string;
}

export type ExtendedRootDb = RootDatabase & {
  committed: () => Promise<void>;
};

export type BucketOptions = DatabaseOptions & {
  indexes?: string[];
};

type DB<V = any, K extends Key = Key> = Database<V, K> & DefaultSerializer<V>;

export interface WrappedDB<V = any, K extends Key = Key>
  extends Omit<Database<V, K>, "put" | "remove">,
    DefaultSerializer<V> {
  put(id: K, value: V, options?: PutOptions): Promise<boolean>;
  remove(id: K, options?: RemoveOptions): Promise<boolean>;
  query(indexName: string, value: any): Promise<V[]>;
}

export class Store<V = any, K extends Key = Key> extends EventEmitter {
  protected env: ExtendedRootDb;
  protected ttlBucket: Database<string, string>;
  protected indexDb: Database<string, K>;
  protected dbs = new Map<string, WrappedDB<any, K>>();
  protected flushing: boolean = false;
  protected indexes = new Map<string, string[]>();

  constructor(name: string, options: RootDatabaseOptions) {
    super();
    this.env = open(name, options) as ExtendedRootDb;
    // Open TTL bucket directly.
    this.ttlBucket = this.env.openDB("ttl", { cache: true });
    this.indexDb = this.env.openDB("index", { cache: true });
  }

  protected ttlKey(exp: number, bucket: string, key: string): string {
    return `${exp}:${bucket}:${key}`;
  }

  async committed() {
    return this.env.committed;
  }

  /**
   * Wrap a raw DB so that it supports custom put, remove, and query logic.
   */
  protected wrapDB<TV>(db: DB<TV, K>, bucketName: string): WrappedDB<TV, K> {
    const wrapped: Partial<WrappedDB<TV, K>> = Object.create(db);

    wrapped.query = async (indexName: string, value: any) => {
      const keys: K[] = [];
      const prefix = this.indexKey(bucketName, indexName, value);
      for (const { value: idxValue } of this.indexDb.getRange({
        start: prefix,
        end: `${prefix}\xff`,
      })) {
        keys.push(idxValue as K);
      }
      const results = await db.getMany(keys);
      return results.filter((item): item is TV => item !== undefined);
    };

    wrapped.put = (
      id: K,
      value: TV,
      options?: PutOptions
    ): Promise<boolean> => {
      let result: Promise<boolean>;
      if (options?.version !== undefined && options.ifVersion !== undefined) {
        result = db.put(id, value, options.version, options.ifVersion);
      } else if (options?.version !== undefined) {
        result = db.put(id, value, options.version);
      } else {
        result = db.put(id, value);
      }

      // Handle TTL: if ttl option is provided, schedule deletion by adding a TTL entry.
      if (options?.ttl) {
        const exp = Date.now() + options.ttl;
        const ttlEntryKey = this.ttlKey(exp, bucketName, String(id));
        this.ttlBucket.put(ttlEntryKey, "");
      }

      // Handle indexes if defined.
      const indexKeys = this.indexes.get(bucketName);
      if (indexKeys) {
        for (const key of indexKeys) {
          const indexVal = (value as any)[key];
          const composite = this.indexKey(bucketName, key, indexVal, id as any);
          this.indexDb.put(composite as K, String(id));
        }
      }

      if (!options?.quiet) {
        this.emit("change", {
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

    wrapped.remove = (id: K, options?: RemoveOptions): Promise<boolean> => {
      const current = db.get(id);

      const indexKeys = this.indexes.get(bucketName);
      if (indexKeys && current) {
        for (const key of indexKeys) {
          const indexVal = (current as any)[key];
          const composite = this.indexKey(bucketName, key, indexVal, id as any);
          this.indexDb.remove(composite as K);
        }
      }

      if (!options?.quiet) {
        this.emit("change", {
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

    return wrapped as WrappedDB<TV, K>;
  }

  /**
   * Generate a composite index key.
   * This implementation uses a simple colon-delimited string,
   * omitting undefined parts.
   */
  private indexKey(bucket: string, index: string, value: any, key?: string) {
    return [bucket, index, value, key].filter((v) => v !== undefined).join(":");
  }

  /**
   * Open or create a bucket in the store.
   */
  bucket<TV = any>(name: string, options?: BucketOptions): WrappedDB<TV, K> {
    let db = this.dbs.get(name);
    if (!db) {
      const opts: DatabaseOptions = { cache: true, ...options };
      const raw = this.env.openDB<TV, K>(name, opts) as DB<TV, K>;
      db = this.wrapDB(raw, name);
      if (options?.indexes) {
        this.indexes.set(name, options.indexes);
      }
      this.dbs.set(name, db);
    }
    return db;
  }

  /**
   * Clean expired entries based on TTL.
   */
  async clean() {
    if (this.flushing) return;
    this.flushing = true;

    const keysToDelete: Array<{
      ttlKey: string;
      bucketName: string;
      id: string;
    }> = [];
    const nowStr = Date.now().toString();

    for (const key of this.ttlBucket.getKeys({ end: nowStr })) {
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

  /**
   * Close all open databases.
   */
  async close() {
    const dbs = Array.from(this.dbs.values()).map((db) => db.close());
    await Promise.all([...dbs, this.env.close(), this.ttlBucket.close()]);
  }
}
