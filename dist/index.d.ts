import { EventEmitter } from "events";
import {
  Key,
  Database,
  RootDatabase,
  RootDatabaseOptions,
  DatabaseOptions,
} from "lmdbx";

type PutOptions = {
  version?: number;
  ttl?: number;
  ifVersion?: number;
  quiet?: boolean;
};
type RemoveOptions = {
  quiet?: boolean;
  ifVersion?: number;
};
interface DefaultSerializer<V> {
  encoder: {
    encode: (value: V) => Buffer;
  };
  decoder: {
    encode: (value: Buffer) => V;
  };
}

type DB<V = any, K extends Key = Key> = Database<V, K> & DefaultSerializer<V>;

interface WrappedDB<V = any, K extends Key = Key>
  extends Omit<Database<V, K>, "put" | "remove">,
    DefaultSerializer<V> {
  put(id: K, value: V, options?: PutOptions): Promise<boolean>;
  remove(id: K, options?: RemoveOptions): Promise<boolean>;
}
declare class Store<V = any, K extends Key = Key> extends EventEmitter {
  protected env: RootDatabase;
  protected ttlBucket: Database<string, string>;
  protected dbs: Map<string, WrappedDB<any, K>>;
  protected flushing: boolean;
  constructor(name: string, options: RootDatabaseOptions);
  protected ttlKey(exp: number, bucket: string, key: string): string;
  /**
   * Wrap a given DB instance using a levelup-style approach.
   * All methods and properties are available via the prototype,
   * but we override put and remove with our custom implementations.
   */
  protected wrapDB<TV>(db: DB<TV, K>, bucketName: string): WrappedDB<TV, K>;
  /**
   * Retrieve or create a sub-database.
   * The returned instance is wrapped so that our custom put/remove
   * (with TTL and change event functionality) are available.
   */
  bucket<TV = any>(name: string, options?: DatabaseOptions): WrappedDB<TV, K>;
  /**
   * Clean up expired TTL entries in batch.
   */
  clean(): Promise<void>;
}

export { type PutOptions, type RemoveOptions, Store, type WrappedDB };
