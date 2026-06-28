'use strict'

// Buckets — mmap storage engine
// Standalone, all hot paths inlined, no inheritance overhead
// RW mmap for in-place overwrites, MAP_POPULATE + MADV_RANDOM

const fs = require('fs')
const path = require('path')
const { hash, keyTag, HEADER_SIZE, SLOT_SIZE, FRAME_SIZE,
        TOMBSTONE_MARKER, EXPIRY_FLAG, EXPIRY_BYTES, MAX_KEY_LEN, MAX_PREFIX_LEN,
        writeHeader, readHeader, writeSlot,
        EMPTY, USED, TOMB } = require('./disk')

const IS_BUN = typeof globalThis.Bun !== 'undefined'
let native
if (!IS_BUN) { try { native = require('mmap-native') } catch {} }

const DEFAULT_CAPACITY = 1 << 12  // 4096 — grows automatically via _resize
const LOAD_FACTOR = 0.7

function assertKeyLen(kLen) {
  if (kLen === 0 || kLen > MAX_KEY_LEN) throw new RangeError('key length must be 1-' + MAX_KEY_LEN)
}

function mmapCreate(fdOrPath, size, writable) {
  if (size === 0) return null
  if (IS_BUN) {
    const arr = Bun.mmap(fdOrPath, { shared: true })
    return Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength)
  }
  if (!native) throw new Error('mmap requires Bun or native addon (cd native && node-gyp rebuild)')
  return native.mmap(fdOrPath, size, writable ? 1 : 0)
}

function mmapDestroy(buf) {
  // No-op: the native finalizer (mmap_release) handles munmap on GC.
  // This avoids double-free and keeps subarray views valid until GC collects.
  // We intentionally do NOT call native.munmap() here.
}

// Node.js Buffer.compare/subarray use int32 offsets internally,
// which breaks on mmap buffers > 2GB. Use native methods when safe, byte loops for >2GB.
const INT32_MAX = 0x7FFFFFFF
function mmCmpKey(mm, off, key, kLen) {
  if (off + kLen <= INT32_MAX) return mm.compare(key, 0, kLen, off, off + kLen) === 0
  for (let i = 0; i < kLen; i++) { if (mm[off + i] !== key[i]) return false }
  return true
}
function mmSlice(mm, off, len) {
  if (off + len <= INT32_MAX) return mm.subarray(off, off + len)
  const out = Buffer.allocUnsafe(len)
  for (let i = 0; i < len; i++) out[i] = mm[off + i]
  return out
}

class BucketsDB {
  constructor(dir, opts = {}) {
    this._dir = dir
    this._readOnly = !!opts.readOnly

    let capacity = opts.capacity || DEFAULT_CAPACITY
    capacity = 1 << Math.ceil(Math.log2(Math.max(capacity, 16)))

    this.capacity = capacity
    this.mask = capacity - 1
    this.size = 0
    this.tombstones = 0
    this.maxLoad = (capacity * LOAD_FACTOR) | 0

    // In-memory index — typed arrays for cache-friendly probing
    this._hashes = new Uint32Array(capacity)
    this._flags = new Uint8Array(capacity)
    this._keyTags = new Uint32Array(capacity)
    this._offsets = new Float64Array(capacity)
    this._keyLens = new Uint16Array(capacity)
    this._valLens = new Uint32Array(capacity)
    this._expiries = new Float64Array(capacity)

    // Data file
    this._dataFd = null
    this._dataPath = null
    this._dataSize = 0
    this._diskSize = 0

    // mmap
    this._mmapBuf = null
    this._mmapSize = 0

    // Mutation counter for cursor cache invalidation
    this._gen = 0

    // Write buffer
    this._wBuf = Buffer.allocUnsafe(1 << 18) // 256KB write buffer
    this._wPos = 0

    // Lock
    this._lockPath = null

    if (dir) this._openDir()
  }

  // ---- GET — fully inlined, zero-copy from mmap ----

  get(key) {
    if (typeof key === 'string') key = Buffer.from(key)
    const kLen = key.length
    assertKeyLen(kLen)

    // Inline hash — avoid function call overhead
    let h = 0x811c9dc5
    let hi = 0
    for (; hi + 3 < kLen; hi += 4) {
      h = Math.imul(h ^ (key[hi] | (key[hi + 1] << 8) | (key[hi + 2] << 16) | (key[hi + 3] << 24)), 0x5bd1e995)
    }
    for (; hi < kLen; hi++) h = Math.imul(h ^ key[hi], 0x5bd1e995)
    h ^= h >>> 13; h = Math.imul(h, 0xc2b2ae35); h = (h ^ (h >>> 16)) >>> 0

    // Inline keyTag
    const tag = kLen >= 4
      ? (key[0] | (key[1] << 8) | (key[2] << 16) | (key[3] << 24)) >>> 0
      : (key[0] | (kLen > 1 ? key[1] << 8 : 0) | (kLen > 2 ? key[2] << 16 : 0)) >>> 0

    const flags = this._flags
    const hashes = this._hashes
    const keyTags = this._keyTags
    const keyLens = this._keyLens
    const mask = this.mask
    // Lazy mmap: create on first read, don't remap on every flush
    if (!this._mmapBuf && this._diskSize > 0) { if (this._wPos > 0) this._flushWrite(); this._remap() }
    const mm = this._mmapBuf
    const mmSize = this._mmapSize
    let idx = h & mask

    for (;;) {
      const f = flags[idx]
      if (f === EMPTY) return null
      if (f === USED && hashes[idx] === h && keyTags[idx] === tag && keyLens[idx] === kLen) {
        const dOff = this._offsets[idx]
        const vLen = this._valLens[idx]
        const end = dOff + kLen + vLen

        if (mm && end <= mmSize) {
          if (mm.compare(key, 0, kLen, dOff, dOff + kLen) === 0) {
            const exp = this._expiries[idx]
            if (exp !== 0 && Date.now() >= exp) {
              flags[idx] = TOMB; this.size--; this.tombstones++; return null
            }
            return mm.subarray(dOff + kLen, dOff + kLen + vLen)
          }
        } else {
          // Cold tail: pread fallback
          if (end > this._diskSize) this._flushWrite()
          const total = kLen + vLen
          const buf = Buffer.allocUnsafe(total)
          fs.readSync(this._dataFd, buf, 0, total, dOff)
          if (buf.compare(key, 0, kLen, 0, kLen) === 0) {
            const exp = this._expiries[idx]
            if (exp !== 0 && Date.now() >= exp) {
              flags[idx] = TOMB; this.size--; this.tombstones++; return null
            }
            return buf.subarray(kLen)
          }
        }
      }
      idx = (idx + 1) & mask
    }
  }

  // ---- PUT — in-place overwrite via mmap when value fits ----

  put(key, value, opts) {
    this._assertWritable()
    this._gen++
    if (typeof key === 'string') key = Buffer.from(key)
    if (typeof value === 'string') value = Buffer.from(value)
    const kLen = key.length
    assertKeyLen(kLen)
    const vLen = value.length

    // Inline hash
    let h = 0x811c9dc5, hi = 0
    for (; hi + 3 < kLen; hi += 4) {
      h = Math.imul(h ^ (key[hi] | (key[hi + 1] << 8) | (key[hi + 2] << 16) | (key[hi + 3] << 24)), 0x5bd1e995)
    }
    for (; hi < kLen; hi++) h = Math.imul(h ^ key[hi], 0x5bd1e995)
    h ^= h >>> 13; h = Math.imul(h, 0xc2b2ae35); h = (h ^ (h >>> 16)) >>> 0

    const tag = kLen >= 4
      ? (key[0] | (key[1] << 8) | (key[2] << 16) | (key[3] << 24)) >>> 0
      : (key[0] | (kLen > 1 ? key[1] << 8 : 0) | (kLen > 2 ? key[2] << 16 : 0)) >>> 0

    const expiry = opts ? (opts.ttl ? Date.now() + opts.ttl : (opts.expires || 0)) : 0
    const flags = this._flags
    const hashes = this._hashes
    const keyTags = this._keyTags
    const keyLens = this._keyLens
    const mask = this.mask
    if (!this._mmapBuf && this._diskSize > 0) { if (this._wPos > 0) this._flushWrite(); this._remap() }
    const mm = this._mmapBuf
    const mmSize = this._mmapSize
    let idx = h & mask
    let firstTomb = -1

    for (;;) {
      const f = flags[idx]

      if (f === EMPTY) {
        const slot = firstTomb !== -1 ? firstTomb : idx
        if (firstTomb !== -1) this.tombstones--
        const dOff = this._appendData(key, value, kLen, vLen, expiry)
        hashes[slot] = h
        flags[slot] = USED
        keyTags[slot] = tag
        this._offsets[slot] = dOff
        keyLens[slot] = kLen
        this._valLens[slot] = vLen
        this._expiries[slot] = expiry
        this.size++
        if (this.size + this.tombstones >= this.maxLoad) this._resize(this.capacity * 2)
        return
      }

      if (f === TOMB) {
        if (firstTomb === -1) firstTomb = idx
        idx = (idx + 1) & mask
        continue
      }

      // USED — check for match
      if (hashes[idx] === h && keyTags[idx] === tag && keyLens[idx] === kLen) {
        const dOff = this._offsets[idx]
        let match = false

        if (mm && dOff + kLen <= mmSize) {
          match = mmCmpKey(mm, dOff, key, kLen)
        } else {
          if (dOff + kLen > this._diskSize) this._flushWrite()
          const tmp = this._rBuf(kLen)
          fs.readSync(this._dataFd, tmp, 0, kLen, dOff)
          match = tmp.compare(key, 0, kLen, 0, kLen) === 0
        }

        if (match) {
          const oldVLen = this._valLens[idx]
          if (vLen <= oldVLen && mm && dOff + kLen + oldVLen <= mmSize) {
            // In-place overwrite directly on mmap — zero syscalls
            const wOff = dOff + kLen
            for (let bi = 0; bi < vLen; bi++) mm[wOff + bi] = value[bi]
            this._valLens[idx] = vLen
          } else {
            // Append new record
            const newOff = this._appendData(key, value, kLen, vLen, expiry)
            this._offsets[idx] = newOff
            this._valLens[idx] = vLen
          }
          this._expiries[idx] = expiry
          return
        }
      }

      idx = (idx + 1) & mask
    }
  }

  // ---- DELETE ----

  delete(key) {
    this._assertWritable()
    this._gen++
    if (typeof key === 'string') key = Buffer.from(key)
    const kLen = key.length
    assertKeyLen(kLen)
    let h = 0x811c9dc5, hi = 0
    for (; hi + 3 < kLen; hi += 4) {
      h = Math.imul(h ^ (key[hi] | (key[hi + 1] << 8) | (key[hi + 2] << 16) | (key[hi + 3] << 24)), 0x5bd1e995)
    }
    for (; hi < kLen; hi++) h = Math.imul(h ^ key[hi], 0x5bd1e995)
    h ^= h >>> 13; h = Math.imul(h, 0xc2b2ae35); h = (h ^ (h >>> 16)) >>> 0
    const tag = kLen >= 4
      ? (key[0] | (key[1] << 8) | (key[2] << 16) | (key[3] << 24)) >>> 0
      : (key[0] | (kLen > 1 ? key[1] << 8 : 0) | (kLen > 2 ? key[2] << 16 : 0)) >>> 0
    const flags = this._flags
    const hashes = this._hashes
    const keyTags = this._keyTags
    const keyLens = this._keyLens
    const mask = this.mask
    if (!this._mmapBuf && this._diskSize > 0) { if (this._wPos > 0) this._flushWrite(); this._remap() }
    const mm = this._mmapBuf
    const mmSize = this._mmapSize
    let idx = h & mask

    for (;;) {
      const f = flags[idx]
      if (f === EMPTY) return false
      if (f === USED && hashes[idx] === h && keyTags[idx] === tag && keyLens[idx] === kLen) {
        const dOff = this._offsets[idx]
        let match = false
        if (mm && dOff + kLen <= mmSize) {
          match = mmCmpKey(mm, dOff, key, kLen)
        } else {
          if (dOff + kLen > this._diskSize) this._flushWrite()
          const tmp = this._rBuf(kLen)
          fs.readSync(this._dataFd, tmp, 0, kLen, dOff)
          match = tmp.compare(key, 0, kLen, 0, kLen) === 0
        }
        if (match) {
          flags[idx] = TOMB
          this.size--
          this.tombstones++
          this._appendTombstone(key, kLen)
          return true
        }
      }
      idx = (idx + 1) & mask
    }
  }

  // ---- HAS ----

  has(key) {
    if (typeof key === 'string') key = Buffer.from(key)
    const kLen = key.length
    assertKeyLen(kLen)
    let h = 0x811c9dc5, hi = 0
    for (; hi + 3 < kLen; hi += 4) {
      h = Math.imul(h ^ (key[hi] | (key[hi + 1] << 8) | (key[hi + 2] << 16) | (key[hi + 3] << 24)), 0x5bd1e995)
    }
    for (; hi < kLen; hi++) h = Math.imul(h ^ key[hi], 0x5bd1e995)
    h ^= h >>> 13; h = Math.imul(h, 0xc2b2ae35); h = (h ^ (h >>> 16)) >>> 0
    const tag = kLen >= 4
      ? (key[0] | (key[1] << 8) | (key[2] << 16) | (key[3] << 24)) >>> 0
      : (key[0] | (kLen > 1 ? key[1] << 8 : 0) | (kLen > 2 ? key[2] << 16 : 0)) >>> 0
    const flags = this._flags
    const hashes = this._hashes
    const keyTags = this._keyTags
    const keyLens = this._keyLens
    const mask = this.mask
    if (!this._mmapBuf && this._diskSize > 0) { if (this._wPos > 0) this._flushWrite(); this._remap() }
    const mm = this._mmapBuf
    const mmSize = this._mmapSize
    let idx = h & mask

    for (;;) {
      const f = flags[idx]
      if (f === EMPTY) return false
      if (f === USED && hashes[idx] === h && keyTags[idx] === tag && keyLens[idx] === kLen) {
        const exp = this._expiries[idx]
        if (exp !== 0 && Date.now() >= exp) {
          flags[idx] = TOMB; this.size--; this.tombstones++; return false
        }
        const dOff = this._offsets[idx]
        if (mm && dOff + kLen <= mmSize) {
          if (mmCmpKey(mm, dOff, key, kLen)) return true
        } else {
          if (dOff + kLen > this._diskSize) this._flushWrite()
          const tmp = this._rBuf(kLen)
          fs.readSync(this._dataFd, tmp, 0, kLen, dOff)
          if (tmp.compare(key, 0, kLen, 0, kLen) === 0) return true
        }
      }
      idx = (idx + 1) & mask
    }
  }

  exists(key) { return this.has(key) }

  getMany(keys) {
    const results = new Array(keys.length)
    for (let i = 0; i < keys.length; i++) results[i] = this.get(keys[i])
    return results
  }

  getManyPacked(keys) {
    const n = keys.length
    const offsets = new Int32Array(n)
    const lengths = new Int32Array(n)
    if (!this._mmapBuf && this._diskSize > 0) { if (this._wPos > 0) this._flushWrite(); this._remap() }
    const mm = this._mmapBuf, mmSize = this._mmapSize

    // First pass: lookup slots, check if all values in mmap
    let total = 0
    let allInMmap = true
    const slots = new Int32Array(n)
    const valOffsets = new Float64Array(n) // data offset of value bytes
    for (let i = 0; i < n; i++) {
      let key = keys[i]
      if (typeof key === 'string') key = Buffer.from(key)
      const kLen = key.length
      assertKeyLen(kLen)
      const h = hash(key, kLen)
      const tag = keyTag(key, kLen)
      const flags = this._flags, hashes = this._hashes, keyTags = this._keyTags, keyLens = this._keyLens
      let idx = h & this.mask
      let found = -1
      for (;;) {
        const f = flags[idx]
        if (f === EMPTY) break
        if (f === USED && hashes[idx] === h && keyTags[idx] === tag && keyLens[idx] === kLen) {
          const exp = this._expiries[idx]
          if (exp !== 0 && Date.now() >= exp) {
            flags[idx] = TOMB; this.size--; this.tombstones++; break
          }
          const dOff = this._offsets[idx]
          let match = false
          if (mm && dOff + kLen <= mmSize) {
            match = mmCmpKey(mm, dOff, key, kLen)
          } else {
            if (dOff + kLen > this._diskSize) this._flushWrite()
            const tmp = this._rBuf(kLen)
            fs.readSync(this._dataFd, tmp, 0, kLen, dOff)
            match = tmp.compare(key, 0, kLen, 0, kLen) === 0
          }
          if (match) { found = idx; break }
        }
        idx = (idx + 1) & this.mask
      }
      slots[i] = found
      if (found !== -1) {
        const vLen = this._valLens[found]
        const vOff = this._offsets[found] + this._keyLens[found]
        lengths[i] = vLen
        valOffsets[i] = vOff
        total += vLen
        if (!(mm && vOff + vLen <= mmSize)) allInMmap = false
      }
    }

    // Zero-copy fast path: all values live in mmap, return mmap as arena
    if (allInMmap && mm) {
      for (let i = 0; i < n; i++) {
        offsets[i] = slots[i] === -1 ? 0 : valOffsets[i]
      }
      return { buffer: mm, offsets, lengths, count: n, zeroCopy: true }
    }

    // Copy path: single arena allocation
    const buffer = Buffer.allocUnsafe(total)
    let pos = 0
    for (let i = 0; i < n; i++) {
      if (slots[i] === -1) { offsets[i] = 0; continue }
      const vLen = lengths[i], vOff = valOffsets[i]
      if (mm && vOff + vLen <= mmSize) {
        for (let bi = 0; bi < vLen; bi++) buffer[pos + bi] = mm[vOff + bi]
      } else {
        fs.readSync(this._dataFd, buffer, pos, vLen, vOff)
      }
      offsets[i] = pos
      pos += vLen
    }

    return { buffer, offsets, lengths, count: n, zeroCopy: false }
  }

  update(key, fn) {
    const val = this.get(key)
    const newVal = fn(val)
    if (newVal === undefined || newVal === null) { this.delete(key); return null }
    const buf = typeof newVal === 'string' ? Buffer.from(newVal) : newVal
    this.put(key, buf)
    return buf
  }

  prefix(pfx) {
    if (typeof pfx === 'string') pfx = Buffer.from(pfx)
    const c = this.cursor()
    c.seek(pfx)
    c._prefix = pfx
    const origValid = c.valid.bind(c)
    c.valid = () => {
      if (!origValid()) return false
      return c.key.length >= pfx.length && c.key.compare(pfx, 0, pfx.length, 0, pfx.length) === 0
    }
    return c
  }

  prefixDelete(pfx) {
    if (typeof pfx === 'string') pfx = Buffer.from(pfx)
    let count = 0
    const toDelete = []
    for (const [key] of this.entries()) {
      if (key.length >= pfx.length && key.compare(pfx, 0, pfx.length, 0, pfx.length) === 0) {
        toDelete.push(Buffer.from(key))
      }
    }
    for (const k of toDelete) { this.delete(k); count++ }
    return count
  }

  reap() {
    const now = Date.now()
    let reaped = 0
    for (let i = 0; i < this.capacity; i++) {
      if (this._flags[i] !== USED) continue
      const exp = this._expiries[i]
      if (exp !== 0 && now >= exp) {
        this._flags[i] = TOMB
        this.size--
        this.tombstones++
        reaped++
      }
    }
    return reaped
  }

  get count() { return this.size }

  clear() {
    this._assertWritable()
    this._flags.fill(0)
    this._expiries.fill(0)
    this.size = 0
    this.tombstones = 0
    this._dataSize = 0
    this._diskSize = 0
    this._wPos = 0
    mmapDestroy(this._mmapBuf)
    this._mmapBuf = null

    this._mmapSize = 0
    if (this._dataFd !== null) fs.ftruncateSync(this._dataFd, 0)
  }

  batch() { return new WriteBatch(this) }

  cursor() { return new Cursor(this) }

  bucket(prefix) { return new Bucket(this, prefix) }

  // ---- Iteration ----

  *entries() {
    this._ensureMmap()
    const mm = this._mmapBuf, mmSize = this._mmapSize, now = Date.now()
    for (let i = 0; i < this.capacity; i++) {
      if (this._flags[i] !== USED) continue
      const exp = this._expiries[i]
      if (exp !== 0 && now >= exp) { this._flags[i] = TOMB; this.size--; this.tombstones++; continue }
      const off = this._offsets[i], kLen = this._keyLens[i], vLen = this._valLens[i]
      if (mm && off + kLen + vLen <= mmSize) {
        yield [mmSlice(mm, off, kLen), mmSlice(mm, off + kLen, vLen)]
      } else {
        const buf = Buffer.allocUnsafe(kLen + vLen)
        fs.readSync(this._dataFd, buf, 0, kLen + vLen, off)
        yield [buf.subarray(0, kLen), buf.subarray(kLen)]
      }
    }
  }

  *keys() {
    this._ensureMmap()
    const mm = this._mmapBuf, mmSize = this._mmapSize, now = Date.now()
    for (let i = 0; i < this.capacity; i++) {
      if (this._flags[i] !== USED) continue
      const exp = this._expiries[i]
      if (exp !== 0 && now >= exp) { this._flags[i] = TOMB; this.size--; this.tombstones++; continue }
      const off = this._offsets[i], kLen = this._keyLens[i]
      if (mm && off + kLen <= mmSize) {
        yield mmSlice(mm, off, kLen)
      } else {
        const buf = Buffer.allocUnsafe(kLen)
        fs.readSync(this._dataFd, buf, 0, kLen, off)
        yield buf
      }
    }
  }

  *values() {
    this._ensureMmap()
    const mm = this._mmapBuf, mmSize = this._mmapSize, now = Date.now()
    for (let i = 0; i < this.capacity; i++) {
      if (this._flags[i] !== USED) continue
      const exp = this._expiries[i]
      if (exp !== 0 && now >= exp) { this._flags[i] = TOMB; this.size--; this.tombstones++; continue }
      const off = this._offsets[i], kLen = this._keyLens[i], vLen = this._valLens[i]
      if (mm && off + kLen + vLen <= mmSize) {
        yield mmSlice(mm, off + kLen, vLen)
      } else {
        const buf = Buffer.allocUnsafe(vLen)
        fs.readSync(this._dataFd, buf, 0, vLen, off + kLen)
        yield buf
      }
    }
  }

  // ---- Range query (sorted scan, O(n)) ----

  range(low, high) {
    if (typeof low === 'string') low = Buffer.from(low)
    if (typeof high === 'string') high = Buffer.from(high)
    const results = []
    for (const [key, value] of this.entries()) {
      if (Buffer.compare(key, low) >= 0 && Buffer.compare(key, high) <= 0) {
        results.push([key, value])
      }
    }
    results.sort((a, b) => Buffer.compare(a[0], b[0]))
    return results
  }

  // ---- Persistence ----

  flush() {
    if (this._readOnly || this._dataFd === null) return
    this._flushWrite()
    fs.fsyncSync(this._dataFd)
    this._saveIndex()
  }

  // Flush write buffer without rewriting index or remapping (fast path for bulk writes)
  // Data beyond current mmap is read via pread fallback until next full flush/remap
  flushWrites() {
    if (this._dataFd === null) return
    this._flushWrite()
  }

  close() {
    if (this._dataFd !== null) {
      this.flush()
      mmapDestroy(this._mmapBuf)
      this._mmapBuf = null

      this._mmapSize = 0
      fs.closeSync(this._dataFd)
      this._dataFd = null
    }
    this._releaseLock()
  }

  compact() {
    this._assertWritable()
    if (this._dataFd === null) return
    this._flushWrite()
    this.reap()
    mmapDestroy(this._mmapBuf)
    this._mmapBuf = null

    this._mmapSize = 0

    const dataPath = path.join(this._dir, 'data.tdb')
    const tmpPath = dataPath + '.tmp'
    const tmpFd = fs.openSync(tmpPath, 'w')
    let newSize = 0

    const frameBuf = Buffer.allocUnsafe(FRAME_SIZE)
    const expBuf = Buffer.allocUnsafe(EXPIRY_BYTES)
    for (let i = 0; i < this.capacity; i++) {
      if (this._flags[i] !== USED) continue
      const kLen = this._keyLens[i], vLen = this._valLens[i]
      const exp = this._expiries[i]
      const hasExpiry = exp > 0
      const total = kLen + vLen
      const buf = Buffer.allocUnsafe(total)
      fs.readSync(this._dataFd, buf, 0, total, this._offsets[i])

      frameBuf.writeUInt16LE(hasExpiry ? (kLen | EXPIRY_FLAG) : kLen, 0)
      frameBuf.writeUInt32LE(vLen, 2)
      fs.writeSync(tmpFd, frameBuf, 0, FRAME_SIZE, newSize)
      this._offsets[i] = newSize + FRAME_SIZE
      fs.writeSync(tmpFd, buf, 0, total, newSize + FRAME_SIZE)
      if (hasExpiry) {
        expBuf.writeDoubleLE(exp, 0)
        fs.writeSync(tmpFd, expBuf, 0, EXPIRY_BYTES, newSize + FRAME_SIZE + total)
      }
      newSize += FRAME_SIZE + total + (hasExpiry ? EXPIRY_BYTES : 0)
    }

    fs.fsyncSync(tmpFd)
    fs.closeSync(tmpFd)
    fs.closeSync(this._dataFd)
    fs.renameSync(tmpPath, dataPath)

    this._dataFd = fs.openSync(dataPath, 'r+')
    this._dataSize = newSize
    this._diskSize = newSize
    this._saveIndex()
  }

  get stats() {
    let liveBytes = 0
    for (let i = 0; i < this.capacity; i++) {
      if (this._flags[i] === USED) liveBytes += FRAME_SIZE + this._keyLens[i] + this._valLens[i] + (this._expiries[i] > 0 ? EXPIRY_BYTES : 0)
    }
    const indexBytes = this.capacity * (4 + 1 + 4 + 8 + 2 + 4 + 8)
    return {
      size: this.size, capacity: this.capacity,
      loadFactor: (this.size + this.tombstones) / this.capacity,
      tombstones: this.tombstones,
      dataFileSize: this._dataSize, garbageBytes: this._dataSize - liveBytes,
      mmapSize: this._mmapSize, indexBytes,
    }
  }

  // ---- Internals ----

  _rbuf = Buffer.allocUnsafe(1 << 16)
  _rBuf(n) {
    if (n > this._rbuf.length) this._rbuf = Buffer.allocUnsafe(n * 2)
    return this._rbuf
  }

  _appendData(key, value, kLen, vLen, expiry) {
    const hasExpiry = expiry > 0
    const total = FRAME_SIZE + kLen + vLen + (hasExpiry ? EXPIRY_BYTES : 0)
    const dOff = this._dataSize
    if (this._wPos + total > this._wBuf.length) {
      this._flushWrite()
      if (total > this._wBuf.length) this._wBuf = Buffer.allocUnsafe(total * 2)
    }
    const w = this._wBuf
    let p = this._wPos
    // Frame header: [u16 keyLen][u32 valLen] — direct byte writes, no validation
    const fk = hasExpiry ? (kLen | EXPIRY_FLAG) : kLen
    w[p] = fk; w[p + 1] = fk >>> 8
    w[p + 2] = vLen; w[p + 3] = vLen >>> 8; w[p + 4] = vLen >>> 16; w[p + 5] = vLen >>> 24
    p += FRAME_SIZE
    key.copy(w, p, 0, kLen); p += kLen
    value.copy(w, p, 0, vLen); p += vLen
    if (hasExpiry) {
      w.writeDoubleLE(expiry, p)
      p += EXPIRY_BYTES
    }
    this._wPos = p
    this._dataSize += total
    return dOff + FRAME_SIZE
  }

  _appendTombstone(key, kLen) {
    const total = FRAME_SIZE + kLen
    if (this._wPos + total > this._wBuf.length) {
      this._flushWrite()
      if (total > this._wBuf.length) this._wBuf = Buffer.allocUnsafe(total * 2)
    }
    const w = this._wBuf
    let p = this._wPos
    w[p] = kLen; w[p + 1] = kLen >>> 8
    w[p + 2] = 0xFF; w[p + 3] = 0xFF; w[p + 4] = 0xFF; w[p + 5] = 0xFF
    p += FRAME_SIZE
    key.copy(w, p, 0, kLen); p += kLen
    this._wPos = p
    this._dataSize += total
  }

  _assertWritable() {
    if (this._readOnly) throw new Error('Buckets: database opened read-only')
  }

  _flushWrite() {
    this._assertWritable()
    if (this._wPos > 0 && this._dataFd !== null) {
      fs.writeSync(this._dataFd, this._wBuf, 0, this._wPos, this._diskSize)
      this._diskSize += this._wPos
      this._wPos = 0
    }
  }

  _resize(newCap) {
    const oldFlags = this._flags, oldHashes = this._hashes, oldKeyTags = this._keyTags
    const oldOffsets = this._offsets, oldKeyLens = this._keyLens, oldValLens = this._valLens
    const oldExpiries = this._expiries, oldCap = this.capacity

    this.capacity = newCap
    this.mask = newCap - 1
    this.maxLoad = (newCap * LOAD_FACTOR) | 0
    this._hashes = new Uint32Array(newCap)
    this._flags = new Uint8Array(newCap)
    this._keyTags = new Uint32Array(newCap)
    this._offsets = new Float64Array(newCap)
    this._keyLens = new Uint16Array(newCap)
    this._valLens = new Uint32Array(newCap)
    this._expiries = new Float64Array(newCap)
    this.size = 0
    this.tombstones = 0

    const mask = this.mask
    for (let i = 0; i < oldCap; i++) {
      if (oldFlags[i] !== USED) continue
      const h = oldHashes[i]
      let idx = h & mask
      while (this._flags[idx] !== EMPTY) idx = (idx + 1) & mask
      this._hashes[idx] = h
      this._flags[idx] = USED
      this._keyTags[idx] = oldKeyTags[i]
      this._offsets[idx] = oldOffsets[i]
      this._keyLens[idx] = oldKeyLens[i]
      this._valLens[idx] = oldValLens[i]
      this._expiries[idx] = oldExpiries[i]
      this.size++
    }
  }

  _ensureMmap() {
    if (this._wPos > 0) this._flushWrite()
    if (this._diskSize > this._mmapSize) this._remap()
  }

  _remap() {
    if (this._diskSize > 0 && this._diskSize > this._mmapSize) {
      mmapDestroy(this._mmapBuf)
      const src = IS_BUN ? this._dataPath : this._dataFd
      this._mmapBuf = mmapCreate(src, this._diskSize, !this._readOnly)
      this._mmapSize = this._diskSize
    }
  }

  // ---- Index persistence ----

  _openDir() {
    if (!this._readOnly) fs.mkdirSync(this._dir, { recursive: true })
    if (!this._readOnly) this._acquireLock()      // read-only opens share the snapshot (no lock)
    this._dataPath = path.join(this._dir, 'data.tdb')
    const idxPath = path.join(this._dir, 'index.tdb')

    if (!this._readOnly && !fs.existsSync(this._dataPath)) fs.writeFileSync(this._dataPath, '')
    this._dataFd = fs.openSync(this._dataPath, this._readOnly ? 'r' : 'r+')

    if (fs.existsSync(idxPath)) {
      this._loadIndex(idxPath)
      const actual = fs.fstatSync(this._dataFd).size
      if (actual < this._dataSize) {
        // Data file shrunk (e.g. clear() crash) — index is stale, rebuild from scratch
        this._flags.fill(0)
        this._expiries.fill(0)
        this.size = 0
        this.tombstones = 0
        this._dataSize = 0
        this._diskSize = actual
        if (actual > 0) this._rebuildTail(0, actual)
      } else {
        this._diskSize = Math.min(actual, this._dataSize)
        if (actual > this._dataSize) this._rebuildTail(this._dataSize, actual)
      }
    } else {
      const actual = fs.fstatSync(this._dataFd).size
      this._dataSize = 0
      this._diskSize = actual
      if (actual > 0) this._rebuildTail(0, actual)
    }

    // Reap expired entries — just sweep flags/expiries arrays, no mmap needed
    const now = Date.now(), flags = this._flags, expiries = this._expiries
    for (let i = 0; i < this.capacity; i++) {
      if (flags[i] === USED && expiries[i] !== 0 && now >= expiries[i]) {
        flags[i] = TOMB; this.size--; this.tombstones++
      }
    }
  }

  _loadIndex(idxPath) {
    // Try fast binary format first (typed array dumps — instant load)
    const fastPath = idxPath.replace('index.tdb', 'index.fast')
    if (fs.existsSync(fastPath)) {
      return this._loadFastIndex(fastPath, idxPath)
    }

    const fd = fs.openSync(idxPath, 'r')
    const hdrBuf = Buffer.allocUnsafe(HEADER_SIZE)
    fs.readSync(fd, hdrBuf, 0, HEADER_SIZE, 0)
    const hdr = readHeader(hdrBuf)

    if (hdr.capacity !== this.capacity) {
      this.capacity = hdr.capacity
      this.mask = hdr.capacity - 1
      this.maxLoad = (hdr.capacity * LOAD_FACTOR) | 0
      this._hashes = new Uint32Array(hdr.capacity)
      this._flags = new Uint8Array(hdr.capacity)
      this._keyTags = new Uint32Array(hdr.capacity)
      this._offsets = new Float64Array(hdr.capacity)
      this._keyLens = new Uint16Array(hdr.capacity)
      this._valLens = new Uint32Array(hdr.capacity)
      this._expiries = new Float64Array(hdr.capacity)
    }

    this.size = hdr.size
    this.tombstones = hdr.tombstones
    this._dataSize = hdr.dataSize

    const totalBytes = hdr.capacity * SLOT_SIZE
    const src = IS_BUN ? idxPath : fd
    const idxMmap = mmapCreate(src, HEADER_SIZE + totalBytes, false)
    if (!IS_BUN) fs.closeSync(fd)

    const cap = hdr.capacity
    const flags = this._flags, hashes = this._hashes, keyTags = this._keyTags
    const offsets = this._offsets, keyLens = this._keyLens, valLens = this._valLens

    const expiries = this._expiries
    const dv = new DataView(idxMmap.buffer, idxMmap.byteOffset, idxMmap.byteLength)
    let off = HEADER_SIZE
    for (let i = 0; i < cap; i++, off += SLOT_SIZE) {
      const f = idxMmap[off + 20]
      if (f === EMPTY) continue
      flags[i] = f
      hashes[i] = (idxMmap[off] | (idxMmap[off+1] << 8) | (idxMmap[off+2] << 16) | (idxMmap[off+3] << 24)) >>> 0
      keyTags[i] = (idxMmap[off+4] | (idxMmap[off+5] << 8) | (idxMmap[off+6] << 16) | (idxMmap[off+7] << 24)) >>> 0
      offsets[i] = ((idxMmap[off+8] | (idxMmap[off+9] << 8) | (idxMmap[off+10] << 16) | (idxMmap[off+11] << 24)) >>> 0) + (idxMmap[off+12] | (idxMmap[off+13] << 8)) * 0x100000000
      keyLens[i] = idxMmap[off+14] | (idxMmap[off+15] << 8)
      valLens[i] = (idxMmap[off+16] | (idxMmap[off+17] << 8) | (idxMmap[off+18] << 16) | (idxMmap[off+19] << 24)) >>> 0
      expiries[i] = dv.getFloat64(off + 22, true)
    }

    // Save fast format for next open
    this._saveFastIndex(fastPath)
  }

  _saveFastIndex(fastPath) {
    // Binary dump: header(32) + flags + hashes + keyTags + keyLens + valLens + offsets
    // All typed arrays dumped raw — load is just Buffer→TypedArray, no parsing
    const cap = this.capacity
    const hdr = Buffer.alloc(32)
    hdr.writeUInt32LE(0x46415354, 0) // 'FAST'
    hdr.writeUInt32LE(cap, 4)
    hdr.writeUInt32LE(this.size, 8)
    hdr.writeUInt32LE(this.tombstones, 12)
    // dataSize as f64 (supports >4GB)
    hdr.writeDoubleLE(this._dataSize, 16)

    const fd = fs.openSync(fastPath + '.tmp', 'w')
    fs.writeSync(fd, hdr)
    fs.writeSync(fd, Buffer.from(this._flags.buffer, this._flags.byteOffset, cap))
    fs.writeSync(fd, Buffer.from(this._hashes.buffer, this._hashes.byteOffset, cap * 4))
    fs.writeSync(fd, Buffer.from(this._keyTags.buffer, this._keyTags.byteOffset, cap * 4))
    fs.writeSync(fd, Buffer.from(this._keyLens.buffer, this._keyLens.byteOffset, cap * 2))
    fs.writeSync(fd, Buffer.from(this._valLens.buffer, this._valLens.byteOffset, cap * 4))
    fs.writeSync(fd, Buffer.from(this._offsets.buffer, this._offsets.byteOffset, cap * 8))
    fs.writeSync(fd, Buffer.from(this._expiries.buffer, this._expiries.byteOffset, cap * 8))
    fs.fsyncSync(fd)
    fs.closeSync(fd)
    fs.renameSync(fastPath + '.tmp', fastPath)
  }

  _loadFastIndex(fastPath, idxPath) {
    const fd = fs.openSync(fastPath, 'r')
    const hdr = Buffer.allocUnsafe(32)
    fs.readSync(fd, hdr, 0, 32, 0)
    if (hdr.readUInt32LE(0) !== 0x46415354) {
      fs.closeSync(fd); fs.unlinkSync(fastPath)
      return this._loadIndex(idxPath)
    }
    const cap = hdr.readUInt32LE(4)
    if (cap !== this.capacity) {
      this.capacity = cap
      this.mask = cap - 1
      this.maxLoad = (cap * LOAD_FACTOR) | 0
    }
    this.size = hdr.readUInt32LE(8)
    this.tombstones = hdr.readUInt32LE(12)
    this._dataSize = hdr.readDoubleLE(16)

    let fileOff = 32

    // Read each array into its own properly-aligned buffer
    this._flags = new Uint8Array(cap)
    const fb = Buffer.from(this._flags.buffer)
    fs.readSync(fd, fb, 0, cap, fileOff); fileOff += cap

    this._hashes = new Uint32Array(cap)
    fs.readSync(fd, Buffer.from(this._hashes.buffer), 0, cap * 4, fileOff); fileOff += cap * 4

    this._keyTags = new Uint32Array(cap)
    fs.readSync(fd, Buffer.from(this._keyTags.buffer), 0, cap * 4, fileOff); fileOff += cap * 4

    this._keyLens = new Uint16Array(cap)
    fs.readSync(fd, Buffer.from(this._keyLens.buffer), 0, cap * 2, fileOff); fileOff += cap * 2

    this._valLens = new Uint32Array(cap)
    fs.readSync(fd, Buffer.from(this._valLens.buffer), 0, cap * 4, fileOff); fileOff += cap * 4

    this._offsets = new Float64Array(cap)
    fs.readSync(fd, Buffer.from(this._offsets.buffer), 0, cap * 8, fileOff); fileOff += cap * 8

    this._expiries = new Float64Array(cap)
    fs.readSync(fd, Buffer.from(this._expiries.buffer), 0, cap * 8, fileOff); fileOff += cap * 8
    fs.closeSync(fd)
  }

  _saveIndex() {
    const idxPath = path.join(this._dir, 'index.tdb')
    const tmpPath = idxPath + '.tmp'
    const fd = fs.openSync(tmpPath, 'w')

    const hdr = Buffer.alloc(HEADER_SIZE)
    writeHeader(hdr, this.capacity, this.size, this.tombstones, this._dataSize)
    fs.writeSync(fd, hdr, 0, HEADER_SIZE)

    const CHUNK = 4096
    const b = Buffer.alloc(CHUNK * SLOT_SIZE)
    for (let base = 0; base < this.capacity; base += CHUNK) {
      const count = Math.min(CHUNK, this.capacity - base)
      b.fill(0, 0, count * SLOT_SIZE)
      let off = 0
      for (let i = 0; i < count; i++, off += SLOT_SIZE) {
        const idx = base + i
        const f = this._flags[idx]
        if (f === EMPTY) continue
        const h = this._hashes[idx]
        b[off] = h; b[off+1] = h >>> 8; b[off+2] = h >>> 16; b[off+3] = h >>> 24
        const t = this._keyTags[idx]
        b[off+4] = t; b[off+5] = t >>> 8; b[off+6] = t >>> 16; b[off+7] = t >>> 24
        const dOff = this._offsets[idx]
        const lo = dOff >>> 0
        b[off+8] = lo; b[off+9] = lo >>> 8; b[off+10] = lo >>> 16; b[off+11] = lo >>> 24
        const hi = (dOff / 0x100000000) >>> 0
        b[off+12] = hi; b[off+13] = hi >>> 8
        const kl = this._keyLens[idx]
        b[off+14] = kl; b[off+15] = kl >>> 8
        const vl = this._valLens[idx]
        b[off+16] = vl; b[off+17] = vl >>> 8; b[off+18] = vl >>> 16; b[off+19] = vl >>> 24
        b[off+20] = f
        const exp = this._expiries[idx]
        if (exp > 0) b.writeDoubleLE(exp, off + 22)
      }
      fs.writeSync(fd, b, 0, count * SLOT_SIZE)
    }

    fs.fsyncSync(fd)
    fs.closeSync(fd)
    fs.renameSync(tmpPath, idxPath)
    // Keep fast index in sync
    this._saveFastIndex(idxPath.replace('index.tdb', 'index.fast'))
  }

  // ---- Crash recovery ----

  _rebuildTail(from, fileSize) {
    const fd = this._dataFd
    const hdr = Buffer.allocUnsafe(FRAME_SIZE)
    let pos = from

    while (pos + FRAME_SIZE <= fileSize) {
      const read = fs.readSync(fd, hdr, 0, FRAME_SIZE, pos)
      if (read < FRAME_SIZE) break
      const rawKLen = hdr.readUInt16LE(0)
      const vLen = hdr.readUInt32LE(2)
      const kLen = rawKLen & ~EXPIRY_FLAG
      const hasExpiry = (rawKLen & EXPIRY_FLAG) !== 0
      const isTombstone = vLen === TOMBSTONE_MARKER

      if (kLen === 0 || kLen > MAX_KEY_LEN) break
      const recordSize = FRAME_SIZE + kLen + (isTombstone ? 0 : vLen + (hasExpiry ? EXPIRY_BYTES : 0))
      if (pos + recordSize > fileSize) break

      const keyOff = pos + FRAME_SIZE
      const keyBuf = Buffer.allocUnsafe(kLen)
      fs.readSync(fd, keyBuf, 0, kLen, keyOff)

      if (isTombstone) {
        this._deleteRecovered(keyBuf, kLen)
      } else {
        let expiry = 0
        if (hasExpiry) {
          const expBuf = Buffer.allocUnsafe(EXPIRY_BYTES)
          fs.readSync(fd, expBuf, 0, EXPIRY_BYTES, keyOff + kLen + vLen)
          expiry = expBuf.readDoubleLE(0)
        }
        this._insertRecovered(keyBuf, kLen, vLen, keyOff, expiry)
      }
      pos += recordSize
    }

    if (pos < fileSize) fs.ftruncateSync(fd, pos)
    this._dataSize = pos
    this._diskSize = pos
  }

  _insertRecovered(keyBuf, kLen, vLen, keyOff, expiry) {
    const h = hash(keyBuf, kLen)
    const tag = keyTag(keyBuf, kLen)
    let idx = h & this.mask
    let firstTomb = -1

    for (;;) {
      const f = this._flags[idx]
      if (f === EMPTY) {
        const slot = firstTomb !== -1 ? firstTomb : idx
        if (firstTomb !== -1) this.tombstones--
        this._hashes[slot] = h
        this._flags[slot] = USED
        this._keyTags[slot] = tag
        this._offsets[slot] = keyOff
        this._keyLens[slot] = kLen
        this._valLens[slot] = vLen
        this._expiries[slot] = expiry
        this.size++
        if (this.size + this.tombstones >= this.maxLoad) this._resize(this.capacity * 2)
        return
      }
      if (f === TOMB) { if (firstTomb === -1) firstTomb = idx; idx = (idx + 1) & this.mask; continue }
      if (this._hashes[idx] === h && this._keyTags[idx] === tag && this._keyLens[idx] === kLen) {
        const tmp = Buffer.allocUnsafe(kLen)
        fs.readSync(this._dataFd, tmp, 0, kLen, this._offsets[idx])
        if (tmp.compare(keyBuf, 0, kLen, 0, kLen) === 0) {
          this._offsets[idx] = keyOff
          this._valLens[idx] = vLen
          this._expiries[idx] = expiry
          return
        }
      }
      idx = (idx + 1) & this.mask
    }
  }

  _deleteRecovered(keyBuf, kLen) {
    const h = hash(keyBuf, kLen)
    const tag = keyTag(keyBuf, kLen)
    let idx = h & this.mask

    for (;;) {
      const f = this._flags[idx]
      if (f === EMPTY) return
      if (f === USED && this._hashes[idx] === h && this._keyTags[idx] === tag && this._keyLens[idx] === kLen) {
        const tmp = Buffer.allocUnsafe(kLen)
        fs.readSync(this._dataFd, tmp, 0, kLen, this._offsets[idx])
        if (tmp.compare(keyBuf, 0, kLen, 0, kLen) === 0) {
          this._flags[idx] = TOMB
          this.size--
          this.tombstones++
          return
        }
      }
      idx = (idx + 1) & this.mask
    }
  }

  // ---- Lock file ----

  _acquireLock() {
    const lockPath = path.join(this._dir, 'buckets.lock')
    try {
      const fd = fs.openSync(lockPath, 'wx')
      fs.writeSync(fd, String(process.pid))
      fs.closeSync(fd)
    } catch (e) {
      if (e.code === 'EEXIST') {
        try {
          const pid = parseInt(fs.readFileSync(lockPath, 'utf8'))
          if (pid && !isNaN(pid)) {
            try { process.kill(pid, 0) } catch (e2) {
              if (e2.code === 'ESRCH') {
                // Dead process — delete stale lock, re-acquire atomically
                try { fs.unlinkSync(lockPath) } catch {}
                const fd2 = fs.openSync(lockPath, 'wx')
                fs.writeSync(fd2, String(process.pid))
                fs.closeSync(fd2)
                this._lockPath = lockPath
                return
              }
            }
          }
        } catch {}
        throw new Error('Database locked by another process')
      }
      throw e
    }
    this._lockPath = lockPath
  }

  _releaseLock() {
    if (this._lockPath) {
      try { fs.unlinkSync(this._lockPath) } catch {}
      this._lockPath = null
    }
  }
}

class WriteBatch {
  constructor(db) { this._db = db; this._ops = [] }
  put(key, value) {
    if (typeof key === 'string') key = Buffer.from(key)
    if (typeof value === 'string') value = Buffer.from(value)
    this._ops.push({ type: 'put', key, value }); return this
  }
  delete(key) {
    if (typeof key === 'string') key = Buffer.from(key)
    this._ops.push({ type: 'del', key }); return this
  }
  commit() {
    for (const op of this._ops) {
      if (op.type === 'put') this._db.put(op.key, op.value)
      else this._db.delete(op.key)
    }
    this._ops.length = 0
  }
  get size() { return this._ops.length }
}

// ---- Cursor — zero-alloc sorted iteration ----

class Cursor {
  constructor(db) {
    this._db = db
    this._sorted = null
    this._pos = -1
    this.key = null
    this.value = null
  }

  _buildSorted() {
    const db = this._db
    db._ensureMmap()
    const mm = db._mmapBuf, mmSize = db._mmapSize
    const entries = []
    const keyCache = new Map()

    const now = Date.now()
    for (let i = 0; i < db.capacity; i++) {
      if (db._flags[i] !== USED) continue
      const exp = db._expiries[i]
      if (exp !== 0 && now >= exp) { db._flags[i] = TOMB; db.size--; db.tombstones++; continue }
      const off = db._offsets[i], kLen = db._keyLens[i]
      let keyBuf
      if (mm && off + kLen <= mmSize) {
        keyBuf = mmSlice(mm, off, kLen)
      } else {
        keyBuf = Buffer.allocUnsafe(kLen)
        fs.readSync(db._dataFd, keyBuf, 0, kLen, off)
      }
      entries.push(i)
      keyCache.set(i, keyBuf)
    }
    entries.sort((a, b) => Buffer.compare(keyCache.get(a), keyCache.get(b)))
    this._sorted = entries
    this._keyCache = keyCache
  }

  seekToFirst() {
    if (!this._sorted) this._buildSorted()
    this._pos = 0
    this._loadCurrent()
    return this
  }

  seekToLast() {
    if (!this._sorted) this._buildSorted()
    this._pos = this._sorted.length - 1
    this._loadCurrent()
    return this
  }

  seek(target) {
    if (typeof target === 'string') target = Buffer.from(target)
    if (!this._sorted) this._buildSorted()
    let lo = 0, hi = this._sorted.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (Buffer.compare(this._keyCache.get(this._sorted[mid]), target) < 0) lo = mid + 1
      else hi = mid
    }
    this._pos = lo
    this._loadCurrent()
    return this
  }

  next() { this._pos++; this._loadCurrent(); return this }
  prev() { this._pos--; this._loadCurrent(); return this }
  valid() { return this._sorted !== null && this._pos >= 0 && this._pos < this._sorted.length }

  _loadCurrent() {
    if (!this.valid()) { this.key = null; this.value = null; return }
    const db = this._db
    const idx = this._sorted[this._pos]
    const off = db._offsets[idx], kLen = db._keyLens[idx], vLen = db._valLens[idx]
    const mm = db._mmapBuf, mmSize = db._mmapSize
    this.key = this._keyCache.get(idx)
    if (mm && off + kLen + vLen <= mmSize) {
      this.value = mmSlice(mm, off + kLen, vLen)
    } else {
      const vBuf = Buffer.allocUnsafe(vLen)
      fs.readSync(db._dataFd, vBuf, 0, vLen, off + kLen)
      this.value = vBuf
    }
  }

  close() { this._sorted = null; this._keyCache = null; this.key = null; this.value = null }

  [Symbol.iterator]() { return this._iter() }
  *_iter() {
    for (this.seekToFirst(); this.valid(); this.next()) yield [this.key, this.value]
    this.close()
  }
}

// ---- Bucket — key-prefixed namespace ----

class Bucket {
  constructor(db, prefix) {
    if (typeof prefix === 'string') prefix = Buffer.from(prefix)
    if (prefix.length > MAX_PREFIX_LEN) throw new RangeError('bucket prefix max ' + MAX_PREFIX_LEN + ' bytes')
    this._db = db
    this._prefix = Buffer.allocUnsafe(1 + prefix.length)
    this._prefix[0] = prefix.length
    prefix.copy(this._prefix, 1)
    this._cursorCache = null

    // Precompute keyTag + mask for fast slot rejection during iteration.
    // Stored keys start with [prefixLen, ...prefixBytes, ...userKey].
    // keyTag is first 4 bytes of the stored key. For prefixes >= 3 bytes,
    // all 4 tag bytes are determined by the prefix alone — zero false positives.
    const p = this._prefix, pLen = p.length // pLen = 1 + prefix.length
    const fixedBytes = Math.min(pLen, 4)
    let tag = 0, mask = 0
    for (let i = 0; i < fixedBytes; i++) {
      tag |= p[i] << (i * 8)
      mask |= 0xFF << (i * 8)
    }
    this._bucketTag = tag >>> 0
    this._bucketMask = mask >>> 0
  }

  _keyBuf = null

  _key(key) {
    if (typeof key === 'string') key = Buffer.from(key)
    const pLen = this._prefix.length
    const totalLen = pLen + key.length
    assertKeyLen(totalLen)
    // Reuse buffer when size matches (common case: fixed-size keys)
    let pk = this._keyBuf
    if (!pk || pk.length !== totalLen) {
      pk = Buffer.allocUnsafe(totalLen)
      this._prefix.copy(pk, 0)
      this._keyBuf = pk
    }
    key.copy(pk, pLen)
    return pk
  }

  _stripPrefix(fullKey) { return fullKey.subarray(this._prefix.length) }

  put(key, value) {
    if (typeof value === 'string') value = Buffer.from(value)
    this._db.put(this._key(key), value)
  }
  get(key) { return this._db.get(this._key(key)) }
  delete(key) { return this._db.delete(this._key(key)) }
  has(key) { return this._db.has(this._key(key)) }

  batch() { return new BucketBatch(this) }
  cursor() { return new BucketCursor(this) }

  *entries() {
    const db = this._db, pfx = this._prefix, pfxLen = pfx.length
    const tag = this._bucketTag, mask = this._bucketMask
    const flags = db._flags, keyTags = db._keyTags, keyLens = db._keyLens
    const cap = db.capacity
    db._ensureMmap()
    const mm = db._mmapBuf, mmSize = db._mmapSize, now = Date.now()
    for (let i = 0; i < cap; i++) {
      if (flags[i] !== USED) continue
      if ((keyTags[i] & mask) !== tag) continue       // fast reject — no mmap touch
      if (keyLens[i] < pfxLen) continue
      const exp = db._expiries[i]
      if (exp !== 0 && now >= exp) { flags[i] = TOMB; db.size--; db.tombstones++; continue }
      const off = db._offsets[i], kLen = keyLens[i], vLen = db._valLens[i]
      let key, value
      if (mm && off + kLen + vLen <= mmSize) {
        if (!mmCmpKey(mm, off, pfx, pfxLen)) continue  // full prefix verify
        key = mmSlice(mm, off + pfxLen, kLen - pfxLen)
        value = mmSlice(mm, off + kLen, vLen)
      } else {
        const buf = Buffer.allocUnsafe(kLen + vLen)
        fs.readSync(db._dataFd, buf, 0, kLen + vLen, off)
        if (buf.compare(pfx, 0, pfxLen, 0, pfxLen) !== 0) continue
        key = buf.subarray(pfxLen, kLen)
        value = buf.subarray(kLen)
      }
      yield [key, value]
    }
  }

  *keys() {
    const db = this._db, pfx = this._prefix, pfxLen = pfx.length
    const tag = this._bucketTag, mask = this._bucketMask
    const flags = db._flags, keyTags = db._keyTags, keyLens = db._keyLens
    const cap = db.capacity
    db._ensureMmap()
    const mm = db._mmapBuf, mmSize = db._mmapSize, now = Date.now()
    for (let i = 0; i < cap; i++) {
      if (flags[i] !== USED) continue
      if ((keyTags[i] & mask) !== tag) continue
      if (keyLens[i] < pfxLen) continue
      const exp = db._expiries[i]
      if (exp !== 0 && now >= exp) { flags[i] = TOMB; db.size--; db.tombstones++; continue }
      const off = db._offsets[i], kLen = keyLens[i]
      if (mm && off + kLen <= mmSize) {
        if (!mmCmpKey(mm, off, pfx, pfxLen)) continue
        yield mmSlice(mm, off + pfxLen, kLen - pfxLen)
      } else {
        const buf = Buffer.allocUnsafe(kLen)
        fs.readSync(db._dataFd, buf, 0, kLen, off)
        if (buf.compare(pfx, 0, pfxLen, 0, pfxLen) !== 0) continue
        yield buf.subarray(pfxLen)
      }
    }
  }

  *values() {
    const db = this._db, pfx = this._prefix, pfxLen = pfx.length
    const tag = this._bucketTag, mask = this._bucketMask
    const flags = db._flags, keyTags = db._keyTags, keyLens = db._keyLens
    const cap = db.capacity
    db._ensureMmap()
    const mm = db._mmapBuf, mmSize = db._mmapSize, now = Date.now()
    for (let i = 0; i < cap; i++) {
      if (flags[i] !== USED) continue
      if ((keyTags[i] & mask) !== tag) continue
      if (keyLens[i] < pfxLen) continue
      const exp = db._expiries[i]
      if (exp !== 0 && now >= exp) { flags[i] = TOMB; db.size--; db.tombstones++; continue }
      const off = db._offsets[i], kLen = keyLens[i], vLen = db._valLens[i]
      if (mm && off + kLen + vLen <= mmSize) {
        if (!mmCmpKey(mm, off, pfx, pfxLen)) continue
        yield mmSlice(mm, off + kLen, vLen)
      } else {
        const buf = Buffer.allocUnsafe(kLen + vLen)
        fs.readSync(db._dataFd, buf, 0, kLen + vLen, off)
        if (buf.compare(pfx, 0, pfxLen, 0, pfxLen) !== 0) continue
        yield buf.subarray(kLen)
      }
    }
  }
}

class BucketBatch {
  constructor(sub) { this._sub = sub; this._ops = [] }
  put(key, value) { this._ops.push({ type: 'put', key, value }); return this }
  delete(key) { this._ops.push({ type: 'del', key }); return this }
  commit() {
    for (const op of this._ops) {
      if (op.type === 'put') this._sub.put(op.key, op.value)
      else this._sub.delete(op.key)
    }
    this._ops.length = 0
  }
  get size() { return this._ops.length }
}

class BucketCursor {
  constructor(sub) {
    this._sub = sub
    this._entries = null
    this._pos = -1
    this.key = null
    this.value = null
  }

  _ensure() {
    if (this._entries) return
    // Reuse cached sorted entries if DB hasn't mutated
    const db = this._sub._db
    const cache = this._sub._cursorCache
    if (cache && cache.gen === db._gen) {
      this._entries = cache.entries
      this._keys = cache.keys
      return
    }
    // Collect slot indices and keys — avoid [k,v] array alloc per entry
    const pfx = this._sub._prefix, pfxLen = pfx.length
    const tag = this._sub._bucketTag, mask = this._sub._bucketMask
    const flags = db._flags, keyTags = db._keyTags, keyLens = db._keyLens
    db._ensureMmap()
    const mm = db._mmapBuf, mmSize = db._mmapSize
    const now = Date.now()
    const slots = []
    const keys = []
    for (let i = 0; i < db.capacity; i++) {
      if (flags[i] !== USED) continue
      if ((keyTags[i] & mask) !== tag) continue
      const exp = db._expiries[i]
      if (exp !== 0 && now >= exp) { flags[i] = TOMB; db.size--; db.tombstones++; continue }
      const off = db._offsets[i], kLen = keyLens[i]
      if (kLen <= pfxLen) continue
      let matchPrefix = true
      if (mm && off + pfxLen <= mmSize) {
        for (let j = 0; j < pfxLen; j++) { if (mm[off + j] !== pfx[j]) { matchPrefix = false; break } }
      } else continue
      if (!matchPrefix) continue
      const userKey = mm.subarray(off + pfxLen, off + kLen)
      slots.push(i)
      keys.push(userKey)
    }
    // Sort by key
    const indices = new Int32Array(slots.length)
    for (let i = 0; i < indices.length; i++) indices[i] = i
    indices.sort((a, b) => Buffer.compare(keys[a], keys[b]))
    const sortedSlots = new Int32Array(indices.length)
    const sortedKeys = new Array(indices.length)
    for (let i = 0; i < indices.length; i++) {
      sortedSlots[i] = slots[indices[i]]
      sortedKeys[i] = keys[indices[i]]
    }
    this._entries = sortedSlots
    this._keys = sortedKeys
    this._sub._cursorCache = { gen: db._gen, entries: sortedSlots, keys: sortedKeys }
  }

  seekToFirst() { this._ensure(); this._pos = 0; this._load(); return this }
  seekToLast() { this._ensure(); this._pos = this._entries.length - 1; this._load(); return this }

  seek(target) {
    if (typeof target === 'string') target = Buffer.from(target)
    this._ensure()
    const keys = this._keys
    let lo = 0, hi = keys.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (Buffer.compare(keys[mid], target) < 0) lo = mid + 1
      else hi = mid
    }
    this._pos = lo
    this._load()
    return this
  }

  next() { this._pos++; this._load(); return this }
  prev() { this._pos--; this._load(); return this }
  valid() { return this._keys && this._pos >= 0 && this._pos < this._keys.length }

  _load() {
    if (!this.valid()) { this.key = null; this.value = null; return }
    this.key = this._keys[this._pos]
    const db = this._sub._db
    const idx = this._entries[this._pos]
    const off = db._offsets[idx], kLen = db._keyLens[idx], vLen = db._valLens[idx]
    const mm = db._mmapBuf, mmSize = db._mmapSize
    if (mm && off + kLen + vLen <= mmSize) {
      this.value = mm.subarray(off + kLen, off + kLen + vLen)
    } else {
      const vBuf = Buffer.allocUnsafe(vLen)
      fs.readSync(db._dataFd, vBuf, 0, vLen, off + kLen)
      this.value = vBuf
    }
  }

  close() { this._entries = null; this._keys = null; this.key = null; this.value = null }

  [Symbol.iterator]() { return this._iter() }
  *_iter() {
    for (this.seekToFirst(); this.valid(); this.next()) yield [this.key, this.value]
    this.close()
  }
}

function open(dir, opts) { return new BucketsDB(dir, opts) }

module.exports = { BucketsDB, WriteBatch, Cursor, Bucket, open }
