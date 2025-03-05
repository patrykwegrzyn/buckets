import { EventEmitter } from 'events';
import { Key, RootDatabase, Database, RootDatabaseOptions, DatabaseOptions } from 'lmdbx';

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
declare class Store<V = any, K extends Key = Key> extends EventEmitter {
    protected env: RootDatabase;
    protected ttlBucket: Database<string, string>;
    protected dbs: Map<string, DB<any, K>>;
    protected flushing: boolean;
    constructor(name: string, options: RootDatabaseOptions);
    protected ttlKey(exp: number, bucket: string, key: string): string;
    protected _patch(db: DB, bucketName: string): void;
    bucket<TV = any>(name: string, options?: DatabaseOptions): DB<TV, K>;
    clean(): Promise<void>;
}

export { type PutOptions, type RemoveOptions, Store };
