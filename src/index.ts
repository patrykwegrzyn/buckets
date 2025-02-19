import { EventEmitter } from "events";
import {
  Database,
  DatabaseOptions,
  Key,
  open,
  RootDatabase,
  RootDatabaseOptions,
} from "lmdbx";

type DB = RootDatabase | Database;

export class Store<V = any, K extends Key = Key> extends EventEmitter {
  protected env: RootDatabase;
  protected dbs: Map<string, Database<any, K>>;

  constructor(name: string, options: RootDatabaseOptions) {
    super();
    this.env = open(name, options);
    this._patch(this.env, "root");
    this.dbs = new Map();
  }

  _patch(db: DB, name: string) {
    const ogput = db.put.bind(db);
    const ogremove = db.remove.bind(db);
    const self = this;

    function _put(id: K, value: V): Promise<boolean>;
    function _put(
      id: K,
      value: V,
      version: number,
      ifVersion?: number
    ): Promise<boolean>;
    function _put(
      id: K,
      value: V,
      versionOrOptions?: number,
      ifVersion?: number
    ): Promise<boolean> {
      let version: number | undefined;
      const result = ogput(id, value, versionOrOptions as number, ifVersion);
      return result.then((res: boolean) => {
        if (typeof versionOrOptions === "number") {
          version = versionOrOptions;
        }

        const value = db.getBinary(id);
        self.emit("change", { type: "put", bucket: name, id, value, version });
        return res;
      });
    }

    function _remove(id: K): Promise<boolean>;
    function _remove(id: K, ifVersion: number): Promise<boolean>;
    function _remove(id: K, valueToRemove: V): Promise<boolean>;
    function _remove(id: K, ifVersionOrValue?: number | V): Promise<boolean> {
      let version: number | undefined;
      if (typeof ifVersionOrValue === "number") {
        version = ifVersionOrValue;
      }

      const value = db.get(id);
      self.emit("change", { type: "remove", bucket: name, id, value, version });
      return ogremove(id, ifVersionOrValue as any);
    }

    db.put = _put as typeof db.put;
    db.remove = _remove as typeof db.remove;
  }

  bucket<TV = any>(name: string, options?: DatabaseOptions): Database<TV, K> {
    let ns = this.dbs.get(name);
    if (!ns) {
      const opts: DatabaseOptions = { cache: true, ...options };
      ns = this.env.openDB<TV, K>(name, opts);
      this.dbs.set(name, ns);
      this._patch(ns, name);
    }
    return ns;
  }
}
