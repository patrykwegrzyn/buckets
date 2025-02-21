"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Store = void 0;
var events_1 = require("events");
var lmdbx_1 = require("lmdbx");
var Store = /** @class */ (function (_super) {
    __extends(Store, _super);
    function Store(name, options) {
        var _this = _super.call(this) || this;
        _this.env = (0, lmdbx_1.open)(name, options);
        _this._patch(_this.env, "root");
        _this.dbs = new Map();
        return _this;
    }
    Store.prototype._patch = function (db, name) {
        var ogput = db.put.bind(db);
        var ogremove = db.remove.bind(db);
        var self = this;
        function _put(id, value, versionOrOptions, ifVersion) {
            var options = {};
            if (typeof versionOrOptions === "number") {
                options.version = versionOrOptions;
            }
            else if (typeof versionOrOptions === "object" &&
                versionOrOptions !== null) {
                options = versionOrOptions;
            }
            var version;
            var result = ogput(id, value, options.version, ifVersion);
            return result.then(function (res) {
                if (typeof versionOrOptions === "number") {
                    version = versionOrOptions;
                }
                var value = db.getBinary(id);
                self.emit("change", { type: "put", bucket: name, id: id, value: value, version: version });
                return res;
            });
        }
        function _remove(id, ifVersionOrValue) {
            var version;
            if (typeof ifVersionOrValue === "number") {
                version = ifVersionOrValue;
            }
            var value = db.get(id);
            self.emit("change", { type: "remove", bucket: name, id: id, value: value, version: version });
            return ogremove(id, ifVersionOrValue);
        }
        db.put = _put;
        db.remove = _remove;
    };
    Store.prototype.bucket = function (name, options) {
        var ns = this.dbs.get(name);
        if (!ns) {
            var opts = __assign({ cache: true }, options);
            ns = this.env.openDB(name, opts);
            this.dbs.set(name, ns);
            this._patch(ns, name);
        }
        return ns;
    };
    return Store;
}(events_1.EventEmitter));
exports.Store = Store;
