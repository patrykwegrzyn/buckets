import { EventEmitter } from 'events';
import { RootDatabase, DatabaseOptions, Key, Database, RootDatabaseOptions } from 'lmdbx';

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
        encode: (value: V) => Buffer | string;
    };
    decoder: {
        decode: (value: Buffer | string) => V;
    };
    encoding: string;
}
type ExtendedRootDb = RootDatabase & {
    committed: () => Promise<void>;
};
type BucketOptions = DatabaseOptions & {
    indexes?: string[];
};
type DB<V = any, K extends Key = Key> = Database<V, K> & DefaultSerializer<V>;
interface WrappedDB<V = any, K extends Key = Key> extends Omit<Database<V, K>, "put" | "remove">, DefaultSerializer<V> {
    put(id: K, value: V, options?: PutOptions): Promise<boolean>;
    remove(id: K, options?: RemoveOptions): Promise<boolean>;
    query(indexName: string, value: any): Promise<V[]>;
}
declare class Store<V = any, K extends Key = Key> extends EventEmitter {
    protected env: ExtendedRootDb;
    protected ttlBucket: Database<string, string>;
    protected indexDb: Database<string, K>;
    protected dbs: Map<string, WrappedDB<any, K>>;
    protected flushing: boolean;
    protected indexes: Map<string, string[]>;
    constructor(name: string, options: RootDatabaseOptions);
    protected ttlKey(exp: number, bucket: string, key: string): string;
    committed(): Promise<() => Promise<void>>;
    /**
     * Wrap a raw DB so that it supports custom put, remove, and query logic.
     */
    protected wrapDB<TV>(db: DB<TV, K>, bucketName: string): WrappedDB<TV, K>;
    /**
     * Generate a composite index key.
     * This implementation uses a simple colon-delimited string,
     * omitting undefined parts.
     */
    private indexKey;
    /**
     * Open or create a bucket in the store.
     */
    bucket<TV = any>(name: string, options?: BucketOptions): WrappedDB<TV, K>;
    /**
     * Clean expired entries based on TTL.
     */
    clean(): Promise<void>;
    /**
     * Close all open databases.
     */
    close(): Promise<void>;
}

export { type BucketOptions, type ExtendedRootDb, type PutOptions, type RemoveOptions, Store, type WrappedDB };
