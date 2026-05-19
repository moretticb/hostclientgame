// room.js — minimal Nostr-backed room layer for browser games.
//
// One room = one pub/sub topic on a set of public Nostr relays. Anyone who
// knows the room id sees everyone's messages. Suitable for small groups
// (handfuls of peers), not crowd-scale.
//
// Provides:
//   - Identity per tab (ephemeral schnorr keypair)
//   - Typed pub/sub: room.send(type, payload) / room.on(type, handler)
//   - Presence: live peer list with lastSeen, join/leave callbacks
//   - Host election: lowest pubkey hex among active peers is host
//
// Does NOT provide:
//   - Snapshots, state sync, conflict resolution — per-game, app layer
//   - Encryption — events are signed but plaintext; anyone with the room id
//     can read them
//   - Persistence — uses ephemeral Nostr kinds (relays don't store)

import { schnorr } from 'https://esm.sh/@noble/secp256k1@3.1.0'

const DEFAULT_RELAYS = [
  'wss://nostr.tegila.com.br',   // Brazil — primary
  'wss://relay.damus.io',
  'wss://nos.lol',
  'wss://relay.nostr.band',
]

const utf8 = new TextEncoder()
const bytesToHex = b => Array.from(b, x => x.toString(16).padStart(2, '0')).join('')

export function joinRoom({
  roomId,
  relays        = DEFAULT_RELAYS,
  kind          = 20100,
  heartbeatMs   = 4000,
  peerTimeoutMs = 12000,
} = {}) {
  if (!roomId) throw new Error('roomId is required')

  const { secretKey, publicKey } = schnorr.keygen()
  const selfId = bytesToHex(publicKey)

  const peers    = Object.create(null)     // pubkey -> { lastSeen }
  const handlers = Object.create(null)     // type -> Set<fn>
  const sockets  = []
  const seenIds  = new Set()
  let   hostId   = null

  const onHostChange = []
  const onPeerJoin   = []
  const onPeerLeave  = []
  const onConnect    = []

  const emit = (list, ...a) => { for (const fn of list) try { fn(...a) } catch (e) { console.error(e) } }
  const off  = (list, fn)   => { const i = list.indexOf(fn); if (i >= 0) list.splice(i, 1) }

  function recomputeHost() {
    const next = Object.keys(peers).sort()[0] || null
    if (next === hostId) return
    const prev = hostId
    hostId = next
    emit(onHostChange, hostId, prev)
  }

  function touch(pubkey) {
    if (peers[pubkey]) { peers[pubkey].lastSeen = Date.now(); return }
    peers[pubkey] = { lastSeen: Date.now() }
    if (pubkey !== selfId) emit(onPeerJoin, pubkey)
    recomputeHost()
  }

  // ── Nostr protocol ──────────────────────────────────────────────────────
  const SUBID = 'r'
  const subFilter = {
    kinds: [kind],
    '#d':   [roomId],
    since:  Math.floor(Date.now() / 1000) - 5,
  }

  async function buildEvent(content) {
    const e = {
      pubkey:     selfId,
      created_at: Math.floor(Date.now() / 1000),
      kind,
      tags:       [['d', roomId]],
      content:    JSON.stringify(content),
    }
    const serialized = utf8.encode(JSON.stringify(
      [0, e.pubkey, e.created_at, e.kind, e.tags, e.content]
    ))
    const id  = new Uint8Array(await crypto.subtle.digest('SHA-256', serialized))
    const sig = await schnorr.signAsync(id, secretKey)
    return JSON.stringify(['EVENT', { ...e, id: bytesToHex(id), sig: bytesToHex(sig) }])
  }

  async function publish(content) {
    const msg = await buildEvent(content)
    for (const ws of sockets) if (ws.readyState === 1) ws.send(msg)
  }

  const connectedCount = () => sockets.filter(s => s.readyState === 1).length

  function openRelay(url) {
    const ws = new WebSocket(url)
    let backoff = 2000 + Math.random() * 2000
    ws.onopen = () => {
      backoff = 2000
      ws.send(JSON.stringify(['REQ', SUBID, subFilter]))
      emit(onConnect, connectedCount(), url, true)
    }
    ws.onclose = () => {
      emit(onConnect, connectedCount(), url, false)
      setTimeout(() => {
        const idx = sockets.indexOf(ws)
        if (idx >= 0) sockets[idx] = openRelay(url)
      }, backoff)
    }
    ws.onerror = () => {}
    ws.onmessage = ev => {
      let msg; try { msg = JSON.parse(ev.data) } catch { return }
      if (msg[0] !== 'EVENT' || msg[1] !== SUBID) return
      const event = msg[2]
      if (seenIds.has(event.id)) return
      seenIds.add(event.id)
      if (seenIds.size > 5000) seenIds.clear()

      let content; try { content = JSON.parse(event.content) } catch { return }
      touch(event.pubkey)
      if (content.type === '_hi') return                 // internal liveness ping
      const set = handlers[content.type]
      if (!set) return
      const { type: _, ...payload } = content
      for (const fn of set) try { fn(payload, event.pubkey) } catch (e) { console.error(e) }
    }
    return ws
  }

  // ── Bootstrap ───────────────────────────────────────────────────────────
  for (const url of relays) sockets.push(openRelay(url))
  touch(selfId)

  const heartbeat = () => { publish({ type: '_hi' }); setTimeout(heartbeat, heartbeatMs) }
  setTimeout(heartbeat, 400)

  setInterval(() => {
    const cutoff = Date.now() - peerTimeoutMs
    let dropped = false
    for (const [id, p] of Object.entries(peers)) {
      if (id === selfId || p.lastSeen >= cutoff) continue
      delete peers[id]
      emit(onPeerLeave, id)
      dropped = true
    }
    if (dropped) recomputeHost()
  }, 3000)

  // ── Public API ──────────────────────────────────────────────────────────
  return {
    selfId,
    peers,
    get hostId() { return hostId },
    get isHost() { return hostId === selfId },
    get connectedRelays() { return connectedCount() },
    get totalRelays()     { return relays.length },

    send(type, payload = {}) { return publish({ type, ...payload }) },

    on(type, fn) {
      (handlers[type] ||= new Set()).add(fn)
      return () => handlers[type]?.delete(fn)
    },

    onHostChange: fn => { onHostChange.push(fn); return () => off(onHostChange, fn) },
    onPeerJoin:   fn => { onPeerJoin.push(fn);   return () => off(onPeerJoin,   fn) },
    onPeerLeave:  fn => { onPeerLeave.push(fn);  return () => off(onPeerLeave,  fn) },
    onConnect:    fn => { onConnect.push(fn);    return () => off(onConnect,    fn) },
  }
}
