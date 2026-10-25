// Game relay on Cloudflare Workers + one Durable Object.
// Same protocol as relay_server/server.js: forwards Godot multiplayer packets
// between players over WebSocket. Host = peer 1, joiners get ids 2,3,4...
// If the host leaves, the lowest joiner id becomes the new host.
import { DurableObject } from "cloudflare:workers";

const LETTERS = "ABCDEFGHJKMNPQRSTUVWXYZ"; // no I / L / O to avoid confusion

export default {
  async fetch(request, env) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("ok\n", { headers: { "content-type": "text/plain" } });
    }
    return env.RELAY.get(env.RELAY.idFromName("main")).fetch(request);
  },
};

export class Relay extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.rooms = new Map(); // code -> room
    this.clients = new Set();
    this.timer = null;
    this.tick = 0;
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const client = pair[0];
    const ws = pair[1];
    ws.accept();
    this.attach(ws);
    return new Response(null, { status: 101, webSocket: client });
  }

  // ---------- helpers ----------
  makeCode() {
    for (;;) {
      let c = "";
      for (let i = 0; i < 6; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
      if (!this.rooms.has(c)) return c;
    }
  }
  sendJson(ws, obj) {
    try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch (e) {}
  }
  sendFrame(ws, from, payload) {
    try {
      if (!ws || ws.readyState !== 1) return;
      const out = new Uint8Array(4 + payload.length);
      new DataView(out.buffer).setInt32(0, from, true);
      out.set(payload, 4);
      ws.send(out);
    } catch (e) {}
  }
  hasSpace(r) { return r.members.size + 1 < r.max; }

  startTimer() {
    if (this.timer) return;
    this.timer = setInterval(() => {
      this.tick++;
      for (const ws of this.clients) {
        ws.count = 0; // flood window (per second)
        if (this.tick % 25 === 0) this.sendJson(ws, { t: "ping" }); // keep-alive
      }
      if (this.clients.size === 0) { clearInterval(this.timer); this.timer = null; }
    }, 1000);
  }

  // ---------- connection ----------
  attach(ws) {
    ws.room = null;
    ws.id = 0;
    ws.count = 0;
    this.clients.add(ws);
    this.startTimer();
    const hello = setTimeout(() => { if (!ws.room) { try { ws.close(); } catch (e) {} } }, 10000);
    ws.addEventListener("message", (e) => {
      if (++ws.count > 300) { try { ws.close(1008, "flood"); } catch (x) {} return; }
      if (typeof e.data === "string") this.onText(ws, e.data);
      else this.onBinary(ws, e.data);
    });
    const gone = () => {
      clearTimeout(hello);
      this.clients.delete(ws);
      this.leave(ws);
    };
    ws.addEventListener("close", gone);
    ws.addEventListener("error", gone);
  }

  leave(ws) {
    const r = ws.room;
    if (!r) return;
    ws.room = null;
    if (ws === r.host) {
      if (r.members.size === 0) { this.rooms.delete(r.code); return; }
      const ids = [...r.members.keys()].sort((a, b) => a - b);
      const nid = ids[0];
      const nws = r.members.get(nid);
      r.members.delete(nid);
      r.host = nws;
      nws.id = 1;
      this.sendJson(nws, { t: "migrate", new: nid, you: true, others: ids.slice(1) });
      for (const m of r.members.values()) this.sendJson(m, { t: "migrate", new: nid, you: false });
    } else {
      r.members.delete(ws.id);
      this.sendJson(r.host, { t: "out", id: ws.id });
    }
  }

  onText(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    switch (m.t) {
      case "host": {
        if (ws.room) return;
        const max = Math.min(4, Math.max(2, parseInt(m.max) || 4));
        const code = this.makeCode();
        const r = { code, host: ws, members: new Map(), max, pub: m.public !== false, open: true, nextId: 2 };
        this.rooms.set(code, r);
        ws.room = r;
        ws.id = 1;
        this.sendJson(ws, { t: "hosted", code });
        break;
      }
      case "join": {
        if (ws.room) return;
        let r = null;
        const code = String(m.code || "").toUpperCase();
        if (code === "*") {
          const list = [...this.rooms.values()].filter((x) => x.pub && x.open && this.hasSpace(x));
          if (list.length === 0) return this.sendJson(ws, { t: "none" });
          r = list[Math.floor(Math.random() * list.length)];
        } else {
          r = this.rooms.get(code);
          if (!r) return this.sendJson(ws, { t: "err", msg: "Room not found - check the code" });
          if (!r.open) return this.sendJson(ws, { t: "err", msg: "The game already started" });
          if (!this.hasSpace(r)) return this.sendJson(ws, { t: "err", msg: "The room is full" });
        }
        const id = r.nextId++;
        ws.room = r;
        ws.id = id;
        r.members.set(id, ws);
        this.sendJson(ws, { t: "joined", id });
        this.sendJson(r.host, { t: "in", id });
        break;
      }
      case "open":
        if (ws.room && ws === ws.room.host) ws.room.open = !!m.v;
        break;
      case "kick": {
        if (!ws.room || ws !== ws.room.host) return;
        const k = ws.room.members.get(parseInt(m.id));
        if (k) { try { k.close(); } catch (e) {} }
        break;
      }
    }
  }

  onBinary(ws, buf) {
    const r = ws.room;
    if (!r || buf.byteLength < 4) return;
    const target = new DataView(buf).getInt32(0, true);
    const payload = new Uint8Array(buf, 4);
    if (ws === r.host) {
      if (target === 0) for (const m of r.members.values()) this.sendFrame(m, 1, payload);
      else if (target > 1) this.sendFrame(r.members.get(target), 1, payload);
      else if (target < 0) for (const [id, m] of r.members) if (id !== -target) this.sendFrame(m, 1, payload);
    } else {
      this.sendFrame(r.host, ws.id, payload); // joiners only talk to the host
    }
  }
}
