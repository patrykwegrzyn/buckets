import { EventEmitter } from 'events';
import { Key, RootDatabase, Database, RootDatabaseOptions, DatabaseOptions } from 'lmdbx';

type DB = RootDatabase | Database;
declare class Store<V = any, K extends Key = Key> extends EventEmitter {
    protected env: RootDatabase;
    protected dbs: Map<string, Database<any, K>>;
    constructor(name: string, options: RootDatabaseOptions);
    _patch(db: DB, name: string): void;
    bucket<TV = any>(name: string, options?: DatabaseOptions): Database<TV, K>;
}

export { Store };
