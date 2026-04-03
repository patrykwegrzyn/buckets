'use strict'

const fs = require('fs')
const path = require('path')
const { open } = require('./')

const N = parseInt(process.argv[2]) || 200_000
const RUNS = 3
const tmpBase = '/tmp/buckets-bench-' + Date.now()
fs.mkdirSync(tmpBase, { recursive: true })

// ---- Generate data ----
const keyBufs = new Array(N)
const valBufs = new Array(N)
const missKeys = new Array(N)
const shuffled = new Array(N)

for (let i = 0; i < N; i++) {
  const k = Buffer.allocUnsafe(16)
  k.writeUInt32BE(i, 0)
  k.writeUInt32BE((i * 2654435761) >>> 0, 4)
  k.writeUInt32BE(i >>> 8, 8)
  k.writeUInt32BE(0xDEADBEEF, 12)
  keyBufs[i] = k

  const vLen = 64 + ((i * 7) % 192)
  const v = Buffer.allocUnsafe(vLen)
  v.writeUInt32LE(i, 0)
  v.fill(0xAA, 4)
  valBufs[i] = v

  const m = Buffer.allocUnsafe(16)
  m.writeUInt32BE(N + i, 0)
  m.fill(0xFF, 4)
  missKeys[i] = m

  shuffled[i] = i
}
for (let i = N - 1; i > 0; i--) {
  const j = (Math.random() * (i + 1)) | 0
  const t = shuffled[i]; shuffled[i] = shuffled[j]; shuffled[j] = t
}

function med(fn) {
  fn()
  const times = []
  for (let r = 0; r < RUNS; r++) {
    const s = process.hrtime.bigint()
    fn()
    const e = process.hrtime.bigint()
    times.push(Number(e - s) / 1e6)
  }
  times.sort((a, b) => a - b)
  return times[Math.floor(times.length / 2)]
}

function fmtOps(ms) {
  return ((N / ms * 1000) / 1e6).toFixed(2) + 'M ops/s'
}

console.log(`\n=== Buckets Benchmark — ${N.toLocaleString()} entries, 16B keys, 64-256B values ===\n`)

const dir = path.join(tmpBase, 'db')
const db = open(dir, { capacity: N * 2 })

const putMs = med(() => { db.clear(); for (let i = 0; i < N; i++) db.put(keyBufs[i], valBufs[i]); db.flush() })
console.log(`  PUT + flush:        ${fmtOps(putMs)}`)

db.clear(); for (let i = 0; i < N; i++) db.put(keyBufs[i], valBufs[i]); db.flush()
const owMs = med(() => { for (let i = 0; i < N; i++) db.put(keyBufs[i], valBufs[i]); db.flush() })
console.log(`  PUT overwrite:      ${fmtOps(owMs)}`)

db.clear(); for (let i = 0; i < N; i++) db.put(keyBufs[i], valBufs[i]); db.flush()
const getSeqMs = med(() => { for (let i = 0; i < N; i++) db.get(keyBufs[i]) })
console.log(`  GET sequential:     ${fmtOps(getSeqMs)}`)

const getRandMs = med(() => { for (let i = 0; i < N; i++) db.get(keyBufs[shuffled[i]]) })
console.log(`  GET random:         ${fmtOps(getRandMs)}`)

const missMs = med(() => { for (let i = 0; i < N; i++) db.get(missKeys[i]) })
console.log(`  GET miss:           ${fmtOps(missMs)}`)

const mix50Ms = med(() => {
  for (let i = 0; i < N; i++) {
    db.put(keyBufs[shuffled[i]], valBufs[shuffled[i]])
    db.get(keyBufs[shuffled[i]])
  }
})
console.log(`  50/50 PUT/GET:      ${fmtOps(mix50Ms)}`)

const mix90Ms = med(() => {
  for (let i = 0; i < N; i++) {
    if (i % 10 === 0) db.put(keyBufs[shuffled[i]], valBufs[shuffled[i]])
    else db.get(keyBufs[shuffled[i]])
  }
})
console.log(`  90/10 GET/PUT:      ${fmtOps(mix90Ms)}`)

db.close()
fs.rmSync(tmpBase, { recursive: true, force: true })
console.log('')
