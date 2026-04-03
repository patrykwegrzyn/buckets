# Buckets

Fast embedded KV store for Node.js. Mmap-backed, zero-copy reads, no GC pressure on hot paths.

## Install

```
npm install buckets
```

## Quick start

```js
const { open } = require('buckets')

const db = open('/tmp/mydb')

db.put('hello', 'world')
db.get('hello') // Buffer<'world'>
db.delete('hello')

db.close()
```

```js
import { open } from 'buckets'
```

## What you get

```js
// batches
const b = db.batch()
b.put('a', '1')
b.put('b', '2')
b.commit()

// namespaces
const users = db.bucket('users')
users.put('alice', '{"role":"admin"}')

// TTL
db.put('session', 'data', { ttl: 60000 })

// iteration
for (const [key, value] of db.cursor()) {
  console.log(key.toString(), value.toString())
}

// range queries
const results = db.range('a', 'z')

// prefix scan
const c = db.prefix('user:')
for (; c.valid(); c.next()) console.log(c.key.toString())

// bulk reads
const values = db.getMany(['a', 'b', 'c'])
```

## API

**Core** — `put(key, val, opts?)` `get(key)` `has(key)` `delete(key)` `clear()`

**Batch** — `batch()` returns `{ put, delete, commit }`

**Buckets** — `bucket(name)` returns a namespaced view with the same API

**Iteration** — `cursor()` with `seekToFirst()` `seekToLast()` `seek(key)` `next()` `prev()` `valid()` `key` `value`

**Queries** — `range(from, to)` `prefix(prefix)` `prefixDelete(prefix)` `getMany(keys)` `getManyPacked(keys)`

**TTL** — `put(key, val, { ttl: ms })` or `{ expires: timestamp }` then `reap()` to clean up

**Lifecycle** — `flush()` `compact()` `close()`

**Stats** — `size` `count` `stats` `capacity`

## License

MIT
