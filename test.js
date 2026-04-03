'use strict'

const { describe, it, beforeEach, afterEach } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('fs')
const path = require('path')

const { open } = require('./')

const tmpBase = '/tmp/buckets-test-' + process.pid
let tmpN = 0
function tmpDir() { return path.join(tmpBase, String(++tmpN)) }

function cleanup() { try { fs.rmSync(tmpBase, { recursive: true, force: true }) } catch {} }
process.on('exit', cleanup)

{
  const openFn = open
  describe('Buckets', () => {
    let db, dir

    beforeEach(() => {
      dir = tmpDir()
      db = openFn(dir, { capacity: 256 })
    })

    afterEach(() => {
      try { db.close() } catch {}
    })

    // ---- Basic CRUD ----

    describe('basic ops', () => {
      it('put + get', () => {
        db.put(Buffer.from('hello'), Buffer.from('world'))
        const v = db.get(Buffer.from('hello'))
        assert.ok(v)
        assert.equal(v.toString(), 'world')
      })

      it('get miss returns null', () => {
        assert.equal(db.get(Buffer.from('nope')), null)
      })

      it('has', () => {
        db.put(Buffer.from('k'), Buffer.from('v'))
        assert.equal(db.has(Buffer.from('k')), true)
        assert.equal(db.has(Buffer.from('x')), false)
      })

      it('delete', () => {
        db.put(Buffer.from('k'), Buffer.from('v'))
        assert.equal(db.delete(Buffer.from('k')), true)
        assert.equal(db.get(Buffer.from('k')), null)
        assert.equal(db.has(Buffer.from('k')), false)
        assert.equal(db.delete(Buffer.from('k')), false)
      })

      it('overwrite', () => {
        db.put(Buffer.from('k'), Buffer.from('v1'))
        db.put(Buffer.from('k'), Buffer.from('v2'))
        assert.equal(db.get(Buffer.from('k')).toString(), 'v2')
        assert.equal(db.size, 1)
      })

      it('clear', () => {
        for (let i = 0; i < 10; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        db.clear()
        assert.equal(db.size, 0)
        assert.equal(db.get(Buffer.from('k0')), null)
      })

      it('empty value', () => {
        db.put(Buffer.from('k'), Buffer.alloc(0))
        const v = db.get(Buffer.from('k'))
        assert.ok(v)
        assert.equal(v.length, 0)
      })

      it('large values', () => {
        const big = Buffer.alloc(100_000, 0xAB)
        db.put(Buffer.from('big'), big)
        db.flush()
        const v = db.get(Buffer.from('big'))
        assert.ok(v)
        assert.equal(v.length, 100_000)
        assert.equal(v[0], 0xAB)
        assert.equal(v[99_999], 0xAB)
      })
    })

    // ---- Bulk correctness ----

    describe('bulk', () => {
      it('insert and verify 10K entries', () => {
        const N = 10_000
        for (let i = 0; i < N; i++) {
          const k = Buffer.from('key-' + String(i).padStart(6, '0'))
          const v = Buffer.from('val-' + i)
          db.put(k, v)
        }
        assert.equal(db.size, N)

        for (let i = 0; i < N; i++) {
          const k = Buffer.from('key-' + String(i).padStart(6, '0'))
          const v = db.get(k)
          assert.ok(v, `missing key ${i}`)
          assert.equal(v.toString(), 'val-' + i)
        }
      })

      it('overwrite all entries', () => {
        const N = 1000
        for (let i = 0; i < N; i++) db.put(Buffer.from('k' + i), Buffer.from('a'))
        for (let i = 0; i < N; i++) db.put(Buffer.from('k' + i), Buffer.from('b'))
        assert.equal(db.size, N)
        for (let i = 0; i < N; i++) {
          assert.equal(db.get(Buffer.from('k' + i)).toString(), 'b')
        }
      })

      it('delete half, verify other half', () => {
        const N = 1000
        for (let i = 0; i < N; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        for (let i = 0; i < N; i += 2) db.delete(Buffer.from('k' + i))
        assert.equal(db.size, N / 2)
        for (let i = 0; i < N; i++) {
          const v = db.get(Buffer.from('k' + i))
          if (i % 2 === 0) assert.equal(v, null)
          else assert.equal(v.toString(), 'v' + i)
        }
      })
    })

    // ---- Iteration ----

    describe('iteration', () => {
      it('entries', () => {
        db.put(Buffer.from('a'), Buffer.from('1'))
        db.put(Buffer.from('b'), Buffer.from('2'))
        db.flush()
        const entries = [...db.entries()]
        assert.equal(entries.length, 2)
        const keys = entries.map(([k]) => k.toString()).sort()
        assert.deepEqual(keys, ['a', 'b'])
      })

      it('keys', () => {
        db.put(Buffer.from('x'), Buffer.from('1'))
        db.put(Buffer.from('y'), Buffer.from('2'))
        db.flush()
        const keys = [...db.keys()].map(k => k.toString()).sort()
        assert.deepEqual(keys, ['x', 'y'])
      })

      it('values', () => {
        db.put(Buffer.from('a'), Buffer.from('10'))
        db.put(Buffer.from('b'), Buffer.from('20'))
        db.flush()
        const vals = [...db.values()].map(v => v.toString()).sort()
        assert.deepEqual(vals, ['10', '20'])
      })
    })

    // ---- WriteBatch ----

    describe('batch', () => {
      it('commit applies all ops', () => {
        const batch = db.batch()
        batch.put(Buffer.from('a'), Buffer.from('1'))
        batch.put(Buffer.from('b'), Buffer.from('2'))
        batch.delete(Buffer.from('a'))
        batch.commit()
        assert.equal(db.get(Buffer.from('a')), null)
        assert.equal(db.get(Buffer.from('b')).toString(), '2')
      })
    })

    // ---- Persistence + Recovery ----

    describe('persistence', () => {
      it('flush + reopen preserves data', () => {
        db.put(Buffer.from('persist'), Buffer.from('test'))
        db.flush()
        db.close()

        const db2 = openFn(dir)
        assert.equal(db2.get(Buffer.from('persist')).toString(), 'test')
        db2.close()
      })

      it('reopen preserves deletes', () => {
        db.put(Buffer.from('a'), Buffer.from('1'))
        db.put(Buffer.from('b'), Buffer.from('2'))
        db.delete(Buffer.from('a'))
        db.flush()
        db.close()

        const db2 = openFn(dir)
        assert.equal(db2.get(Buffer.from('a')), null)
        assert.equal(db2.get(Buffer.from('b')).toString(), '2')
        assert.equal(db2.size, 1)
        db2.close()
      })

      it('crash recovery — rebuild from framed data (no index)', () => {
        for (let i = 0; i < 100; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        db.flush()
        db.close()

        // Delete the index file to simulate crash
        fs.unlinkSync(path.join(dir, 'index.tdb'))
        // Also remove lock file since we closed
        try { fs.unlinkSync(path.join(dir, 'buckets.lock')) } catch {}

        const db2 = openFn(dir)
        assert.equal(db2.size, 100)
        for (let i = 0; i < 100; i++) {
          const v = db2.get(Buffer.from('k' + i))
          assert.ok(v, `missing k${i} after recovery`)
          assert.equal(v.toString(), 'v' + i)
        }
        db2.close()
      })

      it('crash recovery — partial tail after index checkpoint', () => {
        for (let i = 0; i < 50; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        db.flush()

        // Write more data without flushing index
        for (let i = 50; i < 100; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        // Force data to disk but don't save index
        db._flushWrite()
        fs.fsyncSync(db._dataFd)
        // Close without proper shutdown
        fs.closeSync(db._dataFd)
        db._dataFd = null
        db._releaseLock()

        const db2 = openFn(dir)
        // Should have all 100 entries — 50 from index + 50 recovered from tail
        assert.equal(db2.size, 100)
        for (let i = 0; i < 100; i++) {
          const v = db2.get(Buffer.from('k' + i))
          assert.ok(v, `missing k${i} after tail recovery`)
          assert.equal(v.toString(), 'v' + i)
        }
        db2.close()
      })

      it('truncated record at tail is handled', () => {
        db.put(Buffer.from('safe'), Buffer.from('data'))
        db.flush()
        db.close()

        // Append garbage to data file (partial record)
        const dataPath = path.join(dir, 'data.tdb')
        fs.appendFileSync(dataPath, Buffer.from([0x05, 0x00, 0xFF, 0xFF])) // partial frame

        const db2 = openFn(dir)
        assert.equal(db2.get(Buffer.from('safe')).toString(), 'data')
        db2.close()
      })

      it('crash recovery — deletes survive via tombstone frames', () => {
        db.put(Buffer.from('keep'), Buffer.from('yes'))
        db.put(Buffer.from('kill'), Buffer.from('no'))
        db.flush()
        db.delete(Buffer.from('kill'))
        // Force data (with tombstone frame) to disk, don't save index
        db._flushWrite()
        fs.fsyncSync(db._dataFd)
        fs.closeSync(db._dataFd)
        db._dataFd = null
        db._releaseLock()

        const db2 = openFn(dir)
        assert.equal(db2.get(Buffer.from('keep')).toString(), 'yes')
        assert.equal(db2.get(Buffer.from('kill')), null, 'deleted key resurrected after crash')
        assert.equal(db2.size, 1)
        db2.close()
      })

      it('crash recovery — TTL expiry survives reopen', () => {
        db.put(Buffer.from('ephemeral'), Buffer.from('data'), { expires: Date.now() + 60000 })
        db.flush()
        db.close()

        // Delete index to force full rebuild from framed data
        fs.unlinkSync(path.join(dir, 'index.tdb'))
        try { fs.unlinkSync(path.join(dir, 'buckets.lock')) } catch {}

        const db2 = openFn(dir)
        // Entry should still be alive (expiry is 60s in the future)
        assert.equal(db2.get(Buffer.from('ephemeral')).toString(), 'data')
        db2.close()
      })

      it('crash recovery — expired TTL entries are dead after rebuild', () => {
        db.put(Buffer.from('alive'), Buffer.from('yes'))
        db.put(Buffer.from('dead'), Buffer.from('no'), { expires: Date.now() - 1000 })
        db.flush()
        db.close()

        fs.unlinkSync(path.join(dir, 'index.tdb'))
        try { fs.unlinkSync(path.join(dir, 'buckets.lock')) } catch {}

        const db2 = openFn(dir)
        assert.equal(db2.get(Buffer.from('alive')).toString(), 'yes')
        assert.equal(db2.get(Buffer.from('dead')), null, 'expired TTL entry resurrected')
        db2.close()
      })

      it('stale index after clear() crash — data file shrunk', () => {
        for (let i = 0; i < 20; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        db.flush() // saves index + data

        // Simulate clear() crash: truncate data, don't update index
        db.clear()
        // At this point data.tdb is truncated to 0
        // Don't call flush/close — simulate crash
        fs.closeSync(db._dataFd)
        db._dataFd = null
        db._releaseLock()

        const db2 = openFn(dir)
        // Should have 0 entries — stale index discarded because data file shrunk
        assert.equal(db2.size, 0)
        db2.close()
      })
    })

    // ---- Lock file ----

    describe('lock', () => {
      it('prevents double open', () => {
        assert.throws(() => openFn(dir), /locked/)
      })

      it('stale lock from dead process is stolen', () => {
        db.close()
        // Write a fake lock with a dead PID
        fs.writeFileSync(path.join(dir, 'buckets.lock'), '999999999')
        const db2 = openFn(dir)
        assert.ok(db2.size >= 0)
        db2.close()
      })
    })

    // ---- Range query ----

    describe('range', () => {
      it('returns entries in sorted order', () => {
        db.put(Buffer.from('c'), Buffer.from('3'))
        db.put(Buffer.from('a'), Buffer.from('1'))
        db.put(Buffer.from('e'), Buffer.from('5'))
        db.put(Buffer.from('b'), Buffer.from('2'))
        db.put(Buffer.from('d'), Buffer.from('4'))
        db.flush()

        const r = db.range(Buffer.from('b'), Buffer.from('d'))
        assert.equal(r.length, 3)
        assert.equal(r[0][0].toString(), 'b')
        assert.equal(r[1][0].toString(), 'c')
        assert.equal(r[2][0].toString(), 'd')
      })

      it('empty range', () => {
        db.put(Buffer.from('a'), Buffer.from('1'))
        db.flush()
        const r = db.range(Buffer.from('x'), Buffer.from('z'))
        assert.equal(r.length, 0)
      })
    })

    // ---- Compact ----

    describe('compact', () => {
      it('crash recovery after compact', () => {
        for (let i = 0; i < 50; i++) db.put(Buffer.from('k' + i), Buffer.from('v' + i))
        db.flush()
        for (let i = 0; i < 50; i++) db.put(Buffer.from('k' + i), Buffer.from('updated-' + i))
        db.flush()
        db.compact()
        db.close()

        // Delete index to force rebuild from data file
        fs.unlinkSync(path.join(dir, 'index.tdb'))
        try { fs.unlinkSync(path.join(dir, 'buckets.lock')) } catch {}

        const db2 = openFn(dir)
        assert.equal(db2.size, 50)
        for (let i = 0; i < 50; i++) {
          const v = db2.get(Buffer.from('k' + i))
          assert.ok(v, `missing k${i} after compact + recovery`)
          assert.equal(v.toString(), 'updated-' + i)
        }
        db2.close()
      })

      it('reclaims garbage from overwrites', () => {
        for (let i = 0; i < 100; i++) db.put(Buffer.from('k' + i), Buffer.from('original-' + i))
        db.flush()
        for (let i = 0; i < 100; i++) db.put(Buffer.from('k' + i), Buffer.from('updated-' + i))
        db.flush()

        const before = db.stats
        assert.ok(before.garbageBytes > 0)

        db.compact()
        const after = db.stats
        assert.equal(after.garbageBytes, 0)
        assert.ok(after.dataFileSize < before.dataFileSize)

        // Verify data integrity after compact
        for (let i = 0; i < 100; i++) {
          assert.equal(db.get(Buffer.from('k' + i)).toString(), 'updated-' + i)
        }
      })
    })

    // ---- Edge cases ----

    describe('edge cases', () => {
      it('single byte key and value', () => {
        db.put(Buffer.from([0x00]), Buffer.from([0xFF]))
        assert.equal(db.get(Buffer.from([0x00]))[0], 0xFF)
      })

      it('binary keys with all byte values', () => {
        for (let i = 0; i < 256; i++) {
          const k = Buffer.from([i, i, i, i])
          db.put(k, Buffer.from([i]))
        }
        for (let i = 0; i < 256; i++) {
          const k = Buffer.from([i, i, i, i])
          assert.equal(db.get(k)[0], i)
        }
      })

      it('hash collision handling (same prefix keys)', () => {
        // Keys with same first 4 bytes (same keyTag) but different suffixes
        for (let i = 0; i < 100; i++) {
          const k = Buffer.alloc(8)
          k.writeUInt32LE(0xDEADBEEF, 0)
          k.writeUInt32LE(i, 4)
          db.put(k, Buffer.from('v' + i))
        }
        for (let i = 0; i < 100; i++) {
          const k = Buffer.alloc(8)
          k.writeUInt32LE(0xDEADBEEF, 0)
          k.writeUInt32LE(i, 4)
          assert.equal(db.get(k).toString(), 'v' + i)
        }
      })

      it('auto-resize on load factor', () => {
        const small = openFn(tmpDir(), { capacity: 16 })
        for (let i = 0; i < 100; i++) {
          small.put(Buffer.from('k' + i), Buffer.from('v' + i))
        }
        assert.equal(small.size, 100)
        assert.ok(small.capacity >= 100)
        for (let i = 0; i < 100; i++) {
          assert.equal(small.get(Buffer.from('k' + i)).toString(), 'v' + i)
        }
        small.close()
      })

      it('stats are accurate', () => {
        db.put(Buffer.from('a'), Buffer.from('1'))
        db.put(Buffer.from('b'), Buffer.from('2'))
        db.delete(Buffer.from('a'))
        const s = db.stats
        assert.equal(s.size, 1)
        assert.equal(s.tombstones, 1)
        assert.ok(s.dataFileSize > 0)
      })
    })

    // ---- String convenience ----

    describe('string convenience', () => {
      it('put/get with string keys and values', () => {
        db.put('hello', 'world')
        const v = db.get('hello')
        assert.ok(v)
        assert.equal(v.toString(), 'world')
      })

      it('has/delete with string keys', () => {
        db.put('k', 'v')
        assert.equal(db.has('k'), true)
        assert.equal(db.delete('k'), true)
        assert.equal(db.has('k'), false)
      })

      it('mixed string and buffer', () => {
        db.put('strkey', Buffer.from('bufval'))
        db.put(Buffer.from('bufkey'), 'strval')
        assert.equal(db.get('strkey').toString(), 'bufval')
        assert.equal(db.get(Buffer.from('bufkey')).toString(), 'strval')
      })

      it('batch with strings', () => {
        const b = db.batch()
        b.put('a', '1')
        b.put('b', '2')
        b.commit()
        assert.equal(db.get('a').toString(), '1')
        assert.equal(db.get('b').toString(), '2')
      })

      it('range with strings', () => {
        db.put('a', '1')
        db.put('b', '2')
        db.put('c', '3')
        db.flush()
        const r = db.range('a', 'b')
        assert.equal(r.length, 2)
      })
    })

    // ---- Cursor ----

    describe('cursor', () => {
      it('seekToFirst / next / valid', () => {
        db.put(Buffer.from('c'), Buffer.from('3'))
        db.put(Buffer.from('a'), Buffer.from('1'))
        db.put(Buffer.from('b'), Buffer.from('2'))
        db.flush()

        const c = db.cursor()
        c.seekToFirst()
        assert.ok(c.valid())
        assert.equal(c.key.toString(), 'a')
        assert.equal(c.value.toString(), '1')
        c.next()
        assert.equal(c.key.toString(), 'b')
        c.next()
        assert.equal(c.key.toString(), 'c')
        c.next()
        assert.equal(c.valid(), false)
        c.close()
      })

      it('seekToLast / prev', () => {
        db.put('x', 'X')
        db.put('y', 'Y')
        db.put('z', 'Z')
        db.flush()

        const c = db.cursor()
        c.seekToLast()
        assert.equal(c.key.toString(), 'z')
        c.prev()
        assert.equal(c.key.toString(), 'y')
        c.prev()
        assert.equal(c.key.toString(), 'x')
        c.prev()
        assert.equal(c.valid(), false)
        c.close()
      })

      it('seek to specific key', () => {
        for (let i = 0; i < 10; i++) db.put('k' + i, 'v' + i)
        db.flush()

        const c = db.cursor()
        c.seek('k5')
        assert.ok(c.valid())
        assert.equal(c.key.toString(), 'k5')
        c.close()
      })

      it('seek past all keys', () => {
        db.put('a', '1')
        db.put('b', '2')
        db.flush()

        const c = db.cursor()
        c.seek('z')
        assert.equal(c.valid(), false)
        c.close()
      })

      it('iterator protocol', () => {
        db.put('b', '2')
        db.put('a', '1')
        db.put('c', '3')
        db.flush()

        const entries = [...db.cursor()]
        assert.equal(entries.length, 3)
        assert.equal(entries[0][0].toString(), 'a')
        assert.equal(entries[1][0].toString(), 'b')
        assert.equal(entries[2][0].toString(), 'c')
      })

      it('empty db cursor', () => {
        const c = db.cursor()
        c.seekToFirst()
        assert.equal(c.valid(), false)
        c.close()
      })
    })

    // ---- Sublevel ----

    describe('bucket', () => {
      it('basic CRUD isolated', () => {
        const counts = db.bucket('counts')
        const names = db.bucket('names')

        counts.put('users', '42')
        names.put('alice', 'Alice Smith')

        assert.equal(counts.get('users').toString(), '42')
        assert.equal(names.get('alice').toString(), 'Alice Smith')

        // Cross-bucket isolation
        assert.equal(counts.get('alice'), null)
        assert.equal(names.get('users'), null)
      })

      it('delete within bucket', () => {
        const sub = db.bucket('test')
        sub.put('k', 'v')
        assert.equal(sub.has('k'), true)
        sub.delete('k')
        assert.equal(sub.has('k'), false)
      })

      it('iteration within bucket', () => {
        const sub = db.bucket('ns')
        sub.put('a', '1')
        sub.put('b', '2')
        db.put('other', 'value')  // not in bucket
        db.flush()

        const keys = [...sub.keys()].map(k => k.toString()).sort()
        assert.deepEqual(keys, ['a', 'b'])
        const vals = [...sub.values()].map(v => v.toString()).sort()
        assert.deepEqual(vals, ['1', '2'])
      })

      it('bucket batch', () => {
        const sub = db.bucket('batch')
        const b = sub.batch()
        b.put('x', '10')
        b.put('y', '20')
        b.commit()
        assert.equal(sub.get('x').toString(), '10')
        assert.equal(sub.get('y').toString(), '20')
      })

      it('bucket cursor', () => {
        const sub = db.bucket('cur')
        sub.put('c', '3')
        sub.put('a', '1')
        sub.put('b', '2')
        db.put('zzz', 'outside')  // not in bucket
        db.flush()

        const c = sub.cursor()
        const keys = []
        for (c.seekToFirst(); c.valid(); c.next()) keys.push(c.key.toString())
        assert.deepEqual(keys, ['a', 'b', 'c'])
        c.close()
      })

      it('persistence across reopen', () => {
        const sub = db.bucket('persist')
        sub.put('key', 'val')
        db.flush()
        db.close()

        const db2 = openFn(dir)
        const sub2 = db2.bucket('persist')
        assert.equal(sub2.get('key').toString(), 'val')
        db2.close()
      })

      it('multiple buckets coexist', () => {
        const s1 = db.bucket('a')
        const s2 = db.bucket('b')
        const s3 = db.bucket('c')
        s1.put('k', '1')
        s2.put('k', '2')
        s3.put('k', '3')
        assert.equal(s1.get('k').toString(), '1')
        assert.equal(s2.get('k').toString(), '2')
        assert.equal(s3.get('k').toString(), '3')
      })
    })

    // ---- TTL / Expiry ----

    describe('ttl', () => {
      it('expired key returns null on get', () => {
        db.put('ttl-key', 'val', { ttl: 1 }) // 1ms TTL
        // Busy wait past expiry
        const start = Date.now()
        while (Date.now() - start < 5) {}
        assert.equal(db.get('ttl-key'), null)
      })

      it('expired key returns false on has', () => {
        db.put('ttl-key', 'val', { ttl: 1 })
        const start = Date.now()
        while (Date.now() - start < 5) {}
        assert.equal(db.has('ttl-key'), false)
      })

      it('non-expired key is accessible', () => {
        db.put('ttl-key', 'val', { ttl: 60000 })
        assert.equal(db.get('ttl-key').toString(), 'val')
      })

      it('overwrite resets TTL', () => {
        db.put('k', 'v1', { ttl: 1 })
        db.put('k', 'v2', { ttl: 60000 }) // long TTL
        const start = Date.now()
        while (Date.now() - start < 5) {}
        assert.equal(db.get('k').toString(), 'v2')
      })

      it('absolute expires option', () => {
        db.put('k', 'v', { expires: Date.now() - 1 }) // already expired
        assert.equal(db.get('k'), null)
      })

      it('entries skip expired', () => {
        db.put('alive', 'yes')
        db.put('dead', 'no', { ttl: 1 })
        const start = Date.now()
        while (Date.now() - start < 5) {}
        const keys = [...db.keys()].map(k => k.toString())
        assert.ok(keys.includes('alive'))
        assert.ok(!keys.includes('dead'))
      })

      it('reap removes expired entries', () => {
        db.put('a', '1', { ttl: 1 })
        db.put('b', '2', { ttl: 1 })
        db.put('c', '3') // no TTL
        const start = Date.now()
        while (Date.now() - start < 5) {}
        const reaped = db.reap()
        assert.equal(reaped, 2)
        assert.equal(db.size, 1)
      })

      it('TTL persists across flush + reopen', () => {
        db.put('persist-ttl', 'val', { ttl: 60000 })
        db.put('no-ttl', 'val2')
        db.flush()
        db.close()

        const db2 = openFn(dir)
        // Long TTL should still be alive
        assert.equal(db2.get('persist-ttl').toString(), 'val')
        assert.equal(db2.get('no-ttl').toString(), 'val2')
        db2.close()
      })

      it('expired TTL is dead after reopen', () => {
        db.put('short', 'val', { ttl: 1 })
        db.flush()
        const start = Date.now()
        while (Date.now() - start < 5) {}
        db.close()

        const db2 = openFn(dir)
        assert.equal(db2.get('short'), null)
        db2.close()
      })

      it('cursor skips expired entries', () => {
        db.put('a', '1')
        db.put('b', '2', { ttl: 1 })
        db.put('c', '3')
        db.flush()
        const start = Date.now()
        while (Date.now() - start < 5) {}

        const keys = []
        const c = db.cursor()
        for (c.seekToFirst(); c.valid(); c.next()) keys.push(c.key.toString())
        c.close()
        assert.deepEqual(keys, ['a', 'c'])
      })
    })

    // ---- getMany ----

    describe('getMany', () => {
      it('returns array of results', () => {
        db.put('a', '1')
        db.put('b', '2')
        db.put('c', '3')
        const results = db.getMany(['a', 'b', 'c', 'missing'])
        assert.equal(results[0].toString(), '1')
        assert.equal(results[1].toString(), '2')
        assert.equal(results[2].toString(), '3')
        assert.equal(results[3], null)
      })
    })

    // ---- getManyPacked (arena-style) ----

    describe('getManyPacked', () => {
      it('returns packed arena with offsets and lengths', () => {
        db.put('a', '1111')
        db.put('b', '2222')
        db.put('c', '3333')
        db.flush()

        const { buffer, offsets, lengths, count } = db.getManyPacked(['a', 'b', 'c'])
        assert.equal(count, 3)
        for (let i = 0; i < 3; i++) {
          assert.equal(lengths[i], 4)
          const val = buffer.subarray(offsets[i], offsets[i] + lengths[i]).toString()
          assert.ok(['1111', '2222', '3333'].includes(val))
        }
      })

      it('misses get length 0', () => {
        db.put('x', 'val')
        db.flush()

        const { lengths } = db.getManyPacked(['x', 'nope'])
        assert.ok(lengths[0] > 0)
        assert.equal(lengths[1], 0)
      })

      it('large batch — 1000 keys', () => {
        const keys = []
        for (let i = 0; i < 1000; i++) {
          const k = 'pk' + String(i).padStart(4, '0')
          db.put(k, 'val-' + i)
          keys.push(k)
        }
        db.flush()

        const { buffer, offsets, lengths, count } = db.getManyPacked(keys)
        assert.equal(count, 1000)
        // Spot check a few
        assert.equal(buffer.subarray(offsets[0], offsets[0] + lengths[0]).toString(), 'val-0')
        assert.equal(buffer.subarray(offsets[999], offsets[999] + lengths[999]).toString(), 'val-999')
      })

      it('skips expired TTL entries', () => {
        db.put('alive', 'yes')
        db.put('dead', 'no', { ttl: 1 })
        const start = Date.now()
        while (Date.now() - start < 5) {}
        db.flush()

        const { lengths } = db.getManyPacked(['alive', 'dead'])
        assert.ok(lengths[0] > 0)
        assert.equal(lengths[1], 0)
      })
    })

    // ---- update ----

    describe('update', () => {
      it('read-modify-write', () => {
        db.put('counter', '0')
        db.update('counter', v => String(Number(v.toString()) + 1))
        db.update('counter', v => String(Number(v.toString()) + 1))
        assert.equal(db.get('counter').toString(), '2')
      })

      it('create on miss', () => {
        db.update('new', v => {
          assert.equal(v, null)
          return 'created'
        })
        assert.equal(db.get('new').toString(), 'created')
      })

      it('delete on null return', () => {
        db.put('k', 'v')
        db.update('k', () => null)
        assert.equal(db.get('k'), null)
      })
    })

    // ---- prefix ----

    describe('prefix', () => {
      it('iterates matching prefix only', () => {
        db.put('user:1', 'alice')
        db.put('user:2', 'bob')
        db.put('order:1', 'pizza')
        db.flush()

        const c = db.prefix('user:')
        const keys = []
        for (; c.valid(); c.next()) keys.push(c.key.toString())
        assert.deepEqual(keys, ['user:1', 'user:2'])
        c.close()
      })

      it('empty prefix match', () => {
        db.put('zzzz', 'v')
        db.flush()
        const c = db.prefix('aaa')
        assert.equal(c.valid(), false)
        c.close()
      })
    })

    // ---- prefixDelete ----

    describe('prefixDelete', () => {
      it('deletes all matching keys', () => {
        db.put('session:1', 'a')
        db.put('session:2', 'b')
        db.put('session:3', 'c')
        db.put('user:1', 'keep')
        db.flush()

        const count = db.prefixDelete('session:')
        assert.equal(count, 3)
        assert.equal(db.get('session:1'), null)
        assert.equal(db.get('user:1').toString(), 'keep')
      })
    })

    // ---- exists / count ----

    describe('exists and count', () => {
      it('exists is alias for has', () => {
        db.put('k', 'v')
        assert.equal(db.exists('k'), true)
        assert.equal(db.exists('nope'), false)
      })

      it('count property', () => {
        db.put('a', '1')
        db.put('b', '2')
        assert.equal(db.count, 2)
      })
    })

    // ---- Validation ----

    describe('validation', () => {
      it('rejects keys > 32767 bytes', () => {
        const bigKey = Buffer.alloc(32768, 0x41)
        assert.throws(() => db.put(bigKey, Buffer.from('v')), /key length/)
      })

      it('rejects empty keys', () => {
        assert.throws(() => db.put(Buffer.alloc(0), Buffer.from('v')), /key length/)
      })

      it('rejects bucket prefix > 255 bytes', () => {
        assert.throws(() => db.bucket('a'.repeat(256)), /bucket prefix/)
      })

      it('expired entries have correct size on reopen', () => {
        db.put(Buffer.from('alive'), Buffer.from('yes'))
        db.put(Buffer.from('dead'), Buffer.from('no'), { expires: Date.now() - 1000 })
        db.flush()
        db.close()
        try { fs.unlinkSync(path.join(dir, 'buckets.lock')) } catch {}

        const db2 = openFn(dir)
        // size should be 1 immediately — expired entry reaped on open
        assert.equal(db2.size, 1)
        assert.equal(db2.get(Buffer.from('alive')).toString(), 'yes')
        assert.equal(db2.get(Buffer.from('dead')), null)
        db2.close()
      })
    })

    // ---- Fuzz test ----

    describe('fuzz', () => {
      it('random ops maintain consistency', () => {
        const ref = new Map()
        const N = 5000
        const keys = []
        for (let i = 0; i < 200; i++) {
          const k = Buffer.from('fuzz-' + String(i).padStart(4, '0'))
          keys.push(k)
        }

        for (let i = 0; i < N; i++) {
          const k = keys[(Math.random() * keys.length) | 0]
          const op = Math.random()
          if (op < 0.6) {
            // PUT
            const v = Buffer.from('v' + i)
            db.put(k, v)
            ref.set(k.toString(), v.toString())
          } else if (op < 0.8) {
            // DELETE
            db.delete(k)
            ref.delete(k.toString())
          } else {
            // GET — verify
            const v = db.get(k)
            const expected = ref.get(k.toString())
            if (expected) {
              assert.ok(v, `fuzz: missing key ${k.toString()} at op ${i}`)
              assert.equal(v.toString(), expected, `fuzz: wrong value at op ${i}`)
            } else {
              assert.equal(v, null, `fuzz: ghost key ${k.toString()} at op ${i}`)
            }
          }
        }

        // Final consistency check
        assert.equal(db.size, ref.size)
        for (const [k, v] of ref) {
          const got = db.get(Buffer.from(k))
          assert.ok(got, `fuzz final: missing ${k}`)
          assert.equal(got.toString(), v)
        }
      })

      it('fuzz with flush + reopen cycles', () => {
        const ref = new Map()
        const keys = []
        for (let i = 0; i < 50; i++) keys.push(Buffer.from('rk' + i))

        for (let cycle = 0; cycle < 5; cycle++) {
          for (let i = 0; i < 200; i++) {
            const k = keys[(Math.random() * keys.length) | 0]
            if (Math.random() < 0.7) {
              const v = Buffer.from('c' + cycle + 'i' + i)
              db.put(k, v)
              ref.set(k.toString(), v.toString())
            } else {
              db.delete(k)
              ref.delete(k.toString())
            }
          }

          db.flush()
          db.close()
          db = openFn(dir)

          // Verify after reopen
          for (const [k, v] of ref) {
            const got = db.get(Buffer.from(k))
            assert.ok(got, `cycle ${cycle}: missing ${k}`)
            assert.equal(got.toString(), v)
          }
          assert.equal(db.size, ref.size)
        }
      })
    })
  })
}
