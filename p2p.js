// (was a module script: wrapped so it also runs when the page is opened as a local file)
(async () => { 'use strict';

// Online play for self-hosted copies, with no server of your own.
// Two paths run side by side and players are matched by their own random id:
//  1. direct browser-to-browser (WebRTC via Trystero, found through public Nostr relays): fastest;
//  2. relay through public MQTT brokers over secure websockets: slower, but works on networks that block direct links.
const { joinRoom } = window.Trystero || {};
const APP_ID = 'riftspire-game/v1';
const BROKERS = window.__MQTT_BROKERS || ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt', 'wss://test.mosquitto.org:8081/mqtt'];
const enc = new TextEncoder(), dec = new TextDecoder();
function mqStr(s) { const b = enc.encode(s); return [b.length >> 8, b.length & 255, ...b]; }
function mqPacket(type, body) { const len = []; let n = body.length; do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 128; len.push(d); } while (n > 0); const out = new Uint8Array(1 + len.length + body.length); out[0] = type; out.set(len, 1); out.set(body, 1 + len.length); return out; }
class MiniMqtt {
  constructor(url, topic, onMsg) {
    this.url = url; this.topic = topic; this.onMsg = onMsg; this.open = false; this.buf = new Uint8Array(0); this.dead = false; this.connect();
  }
  connect() {
    if (this.dead) return;
    let ws; try { ws = new WebSocket(this.url, 'mqtt'); } catch (e) { return this.retry(); }
    ws.binaryType = 'arraybuffer'; this.ws = ws;
    ws.onopen = () => { const id = 'rs' + Math.random().toString(36).slice(2, 12); ws.send(mqPacket(0x10, [...mqStr('MQTT'), 4, 2, 0, 60, ...mqStr(id)])); };
    ws.onmessage = ev => { const d = new Uint8Array(ev.data), nb = new Uint8Array(this.buf.length + d.length); nb.set(this.buf); nb.set(d, this.buf.length); this.buf = nb; this.parse(); };
    ws.onclose = ws.onerror = () => { if (this.ws !== ws) return; this.open = false; clearInterval(this.ping); this.ws = null; this.retry(); };
  }
  retry() { if (!this.dead) setTimeout(() => this.connect(), 4000); }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      let mul = 1, len = 0, i = 1, byte; do { if (i >= b.length) return; byte = b[i++]; len += (byte & 127) * mul; mul *= 128; } while (byte & 128);
      if (b.length < i + len) return;
      const type = b[0] >> 4, body = b.subarray(i, i + len); this.buf = b.slice(i + len);
      if (type === 2) { this.open = true; this.ws.send(mqPacket(0x82, [0, 1, ...mqStr(this.topic), 0])); this.ping = setInterval(() => { try { this.ws && this.ws.send(new Uint8Array([0xC0, 0])); } catch (e) {} }, 30000); }
      else if (type === 3) { const tl = (body[0] << 8) | body[1], qos = (b[0] >> 1) & 3, off = 2 + tl + (qos ? 2 : 0); try { this.onMsg(dec.decode(body.subarray(off))); } catch (e) {} }
    }
  }
  pub(s) { if (!this.open || !this.ws || this.ws.bufferedAmount > 200000) return; try { this.ws.send(mqPacket(0x30, [...mqStr(this.topic), ...enc.encode(s)])); } catch (e) {} }
  close() { this.dead = true; try { this.ws && this.ws.send(new Uint8Array([0xE0, 0])); this.ws && this.ws.close(); } catch (e) {} clearInterval(this.ping); }
}
function hash(s) { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return (h >>> 0).toString(36); }
window.__p2p = {
  async join(code) {
    const myId = 'p' + Math.random().toString(36).slice(2, 12);
    const peers = new Map(); // id -> { presence, t, s, direct }
    let mine = null, seq = 0, up = true, dirty = false, lastMq = 0;
    const take = (m, viaDirect) => {
      if (!m || typeof m.i !== 'string' || m.i === myId) return;
      if (m.bye) { peers.delete(m.i); return; }
      const p = peers.get(m.i) || { s: -1, direct: 0 };
      if (viaDirect) p.direct = Date.now();
      if (typeof m.s === 'number' && m.s <= p.s && Date.now() - p.t < 3000) { peers.set(m.i, p); return; }
      p.s = m.s; p.presence = m.p; p.t = Date.now(); peers.set(m.i, p);
    };
    // path 1: direct
    let r = null, act = null, inflight = false, queued = false;
    if (joinRoom) {
      try {
        const cfg = { appId: APP_ID, identity: 'none' }; if (window.__P2P_RELAYS) cfg.relayConfig = { urls: window.__P2P_RELAYS };
        r = joinRoom(cfg, code); act = r.makeAction('pres');
        act.onMessage(d => take(d, true));
        r.onPeerJoin(() => { if (mine) pushDirect(); });
        r.onError && r.onError(() => {});
      } catch (e) { r = null; act = null; }
    }
    const pushDirect = () => {
      if (!up || !act) return; if (inflight) { queued = true; return; }
      inflight = true; act.send({ i: myId, s: seq, p: mine }).catch(() => {}).finally(() => { inflight = false; if (queued) { queued = false; pushDirect(); } });
    };
    // path 2: relay
    const topic = 'riftspire/v2/' + hash(APP_ID + '|' + code);
    const brokers = window.__NO_MQTT ? [] : BROKERS.map(u => new MiniMqtt(u, topic, s => { try { take(JSON.parse(s), false); } catch (e) {} }));
    const mqTimer = setInterval(() => {
      if (!up || !mine) return; const now = Date.now();
      const known = [...peers.values()].filter(p => now - p.t < 6000), allDirect = known.length > 0 && known.every(p => now - p.direct < 3000);
      const gap = allDirect ? 1000 : 90;
      if ((dirty || now - lastMq > 1000) && now - lastMq >= gap) { dirty = false; lastMq = now; const s = JSON.stringify({ i: myId, s: seq, p: mine }); brokers.forEach(b => b.pub(s)); }
    }, 45);
    return {
      peers() {
        const now = Date.now(), out = [{ peer: myId, kind: 'viewer', presence: mine, sameTab: true }];
        for (const [id, p] of peers) { if (!p.presence || now - p.t > 6000) continue; out.push({ peer: id, kind: 'viewer', presence: p.presence }); }
        return out;
      },
      presence(patch) { mine = Object.assign({}, mine || {}, patch); seq++; dirty = true; pushDirect(); return Promise.resolve(); },
      connected() { return up && (brokers.some(b => b.open) || !!act); },
      status() {
        const now = Date.now(), live = [...peers.values()].filter(p => p.presence && now - p.t < 6000);
        return { servers: brokers.filter(b => b.open).length, players: live.length, direct: live.filter(p => now - p.direct < 3000).length };
      },
      leave() {
        up = false; clearInterval(mqTimer); const bye = JSON.stringify({ i: myId, bye: 1 }); brokers.forEach(b => { b.pub(bye); setTimeout(() => b.close(), 150); });
        peers.clear(); return r ? r.leave() : Promise.resolve();
      }
    };
  }
};
window.dispatchEvent(new Event('p2p-ready'));

})();
