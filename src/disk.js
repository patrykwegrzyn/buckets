'use strict'

// Buckets on-disk format
//
// Two files:
//   <dir>/index.tdb  — hash table slots
//   <dir>/data.tdb   — append-only key/value log
//
// Index file layout:
//   [Header: 4096 bytes]
//   [Slots:  capacity * 32 bytes]
//
// Slot layout (32 bytes):
//   0:  hash       u32
//   4:  keyTag     u32
//   8:  dataOffset u32  (offset into data file, low 32 bits)
//   12: dataOffHi  u16  (high 16 bits — supports up to 256TB)
//   14: keyLen     u16
//   16: valLen     u32
//   20: flags      u8   (0=empty, 1=used, 2=tombstone)
//   21: reserved   1 byte
//   22: expiry     f64  (ms timestamp, 0 = no expiry)
//   30: reserved   2 bytes
//
// Data file layout:
//   Record after record, framed for crash recovery
//   Each record: [u16 keyLen][u32 valLen][key bytes][value bytes][optional f64 expiry]
//   keyLen high bit (0x8000) = 8-byte expiry appended after value
//   valLen = 0xFFFFFFFF = tombstone (delete marker, no value/expiry bytes)
//   Index offsets point to key bytes (past the 6-byte frame header)
//
// Header layout (4096 bytes):
//   0:  magic      8 bytes  "TURBODB\0"
//   8:  version    u32      = 1
//   12: capacity   u32      slot count
//   16: size       u32      live entries
//   20: tombstones u32
//   24: dataSize   u32      data file size (low)
//   28: dataSizeHi u32      data file size (high)
//   32: reserved   4064 bytes

const HEADER_SIZE = 4096
const SLOT_SIZE = 32
const FRAME_SIZE = 6 // u16 keyLen + u32 valLen
const MAGIC = Buffer.from('TURBODB\0')
const VERSION = 1
const TOMBSTONE_MARKER = 0xFFFFFFFF
const EXPIRY_FLAG = 0x8000
const EXPIRY_BYTES = 8
const MAX_KEY_LEN = 0x7FFF  // 32767 — high bit reserved for expiry flag
const MAX_PREFIX_LEN = 255  // bucket prefix stored as u8

// Slot field offsets
const S_HASH = 0
const S_KEY_TAG = 4
const S_DATA_OFF = 8
const S_DATA_OFF_HI = 12
const S_KEY_LEN = 14
const S_VAL_LEN = 16
const S_FLAGS = 20
const S_EXPIRY = 22

const EMPTY = 0
const USED = 1
const TOMB = 2

// Hash function (same as in-memory engine)
function hash(buf, len) {
  let h = 0x811c9dc5
  let i = 0
  for (; i + 3 < len; i += 4) {
    h = Math.imul(h ^ (buf[i] | (buf[i + 1] << 8) | (buf[i + 2] << 16) | (buf[i + 3] << 24)), 0x5bd1e995)
  }
  for (; i < len; i++) {
    h = Math.imul(h ^ buf[i], 0x5bd1e995)
  }
  h ^= h >>> 13
  h = Math.imul(h, 0xc2b2ae35)
  h ^= h >>> 16
  return h >>> 0
}

function keyTag(buf, len) {
  if (len >= 4) return (buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24)) >>> 0
  return (buf[0] | (len > 1 ? buf[1] << 8 : 0) | (len > 2 ? buf[2] << 16 : 0)) >>> 0
}

function writeHeader(buf, capacity, size, tombstones, dataSize) {
  MAGIC.copy(buf, 0)
  buf.writeUInt32LE(VERSION, 8)
  buf.writeUInt32LE(capacity, 12)
  buf.writeUInt32LE(size, 16)
  buf.writeUInt32LE(tombstones, 20)
  buf.writeUInt32LE(dataSize % 0x100000000, 24)
  buf.writeUInt32LE((dataSize / 0x100000000) >>> 0, 28)
}

function readHeader(buf) {
  if (buf.compare(MAGIC, 0, 8, 0, 8) !== 0) throw new Error('Invalid Buckets file')
  const version = buf.readUInt32LE(8)
  if (version !== VERSION) throw new Error('Unsupported version: ' + version)
  return {
    capacity: buf.readUInt32LE(12),
    size: buf.readUInt32LE(16),
    tombstones: buf.readUInt32LE(20),
    dataSize: buf.readUInt32LE(24) + buf.readUInt32LE(28) * 0x100000000,
  }
}

function readSlot(buf, off) {
  return {
    hash: buf.readUInt32LE(off + S_HASH),
    keyTag: buf.readUInt32LE(off + S_KEY_TAG),
    dataOffset: buf.readUInt32LE(off + S_DATA_OFF) + buf.readUInt16LE(off + S_DATA_OFF_HI) * 0x100000000,
    keyLen: buf.readUInt16LE(off + S_KEY_LEN),
    valLen: buf.readUInt32LE(off + S_VAL_LEN),
    flags: buf[off + S_FLAGS],
    expiry: buf.readDoubleLE(off + S_EXPIRY),
  }
}

function writeSlot(buf, off, h, tag, dataOffset, kLen, vLen, flags, expiry) {
  buf.writeUInt32LE(h, off + S_HASH)
  buf.writeUInt32LE(tag, off + S_KEY_TAG)
  buf.writeUInt32LE(dataOffset % 0x100000000, off + S_DATA_OFF)
  buf.writeUInt16LE((dataOffset / 0x100000000) >>> 0, off + S_DATA_OFF_HI)
  buf.writeUInt16LE(kLen, off + S_KEY_LEN)
  buf.writeUInt32LE(vLen, off + S_VAL_LEN)
  buf[off + S_FLAGS] = flags
  buf.writeDoubleLE(expiry || 0, off + S_EXPIRY)
}

module.exports = {
  HEADER_SIZE, SLOT_SIZE, FRAME_SIZE, MAGIC, VERSION,
  TOMBSTONE_MARKER, EXPIRY_FLAG, EXPIRY_BYTES, MAX_KEY_LEN, MAX_PREFIX_LEN,
  S_HASH, S_KEY_TAG, S_DATA_OFF, S_DATA_OFF_HI, S_KEY_LEN, S_VAL_LEN, S_FLAGS, S_EXPIRY,
  EMPTY, USED, TOMB,
  hash, keyTag, writeHeader, readHeader, readSlot, writeSlot,
}
