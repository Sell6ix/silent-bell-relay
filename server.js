// Сервер-посредник «Молчащего Колокола».
// Хост открывает комнату и получает код. Друзья входят по коду. Сервер только пересылает байты:
// логика игры остаётся у хоста.
//
// Протокол (WebSocket):
//   текст  клиент → сервер  {"op":"host"}            → {"op":"hosted","room":"K7QXM"}
//          клиент → сервер  {"op":"join","room":"…"} → {"op":"joined","id":N}; хосту {"op":"peer","id":N}
//          сервер → хост    {"op":"gone","id":N}      друг отключился
//          сервер → любой   {"op":"error","msg":"…"}
//   байты  хост → сервер    [uint32 LE кому (0 = всем)] + данные → другу приходят только данные
//          друг → сервер    данные → хосту приходит [uint32 LE от кого] + данные
import http from "node:http";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8080);
const MAX_ROOMS = Number(process.env.MAX_ROOMS || 500);
const MAX_GUESTS = 3;
const MAX_MSG = 8 * 1024 * 1024;
const ABC = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

/** @type {Map<string, {host: import("ws").WebSocket, guests: Map<number, import("ws").WebSocket>, next: number}>} */
const rooms = new Map();

const log = (...a) => console.log(new Date().toISOString(), ...a);

function newCode() {
  for (;;) {
    let s = "";
    for (let i = 0; i < 5; i++) s += ABC[Math.floor(Math.random() * ABC.length)];
    if (!rooms.has(s)) return s;
  }
}

const send = (ws, obj) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj)); };
// Закрываем с паузой: иначе клиент может не успеть прочитать текст ошибки.
const fail = (ws, msg) => { send(ws, { op: "error", msg }); setTimeout(() => ws.close(4000, "error"), 300); };

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end(`silent-bell relay ok · комнат: ${rooms.size}\n`);
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });

wss.on("connection", (ws, req) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
  let role = null;   // "host" | "guest"
  let room = null;   // код комнаты
  let id = 0;        // номер друга в комнате

  ws.on("message", (data, isBinary) => {
    if (!isBinary) {
      let m;
      try { m = JSON.parse(data.toString()); } catch { return fail(ws, "Неверное сообщение"); }
      if (role) return; // управление только в начале
      if (m.op === "host") {
        if (rooms.size >= MAX_ROOMS) return fail(ws, "Сервер переполнен, попробуйте позже");
        room = newCode();
        role = "host";
        rooms.set(room, { host: ws, guests: new Map(), next: 1 });
        send(ws, { op: "hosted", room });
        log("комната", room, "открыта");
      } else if (m.op === "join") {
        const code = String(m.room || "").trim().toUpperCase();
        const r = rooms.get(code);
        if (!r) return fail(ws, `Комната ${code} не найдена. Проверьте код — хост должен быть в игре.`);
        if (r.guests.size >= MAX_GUESTS) return fail(ws, "Комната заполнена");
        role = "guest";
        room = code;
        id = r.next++;
        r.guests.set(id, ws);
        send(ws, { op: "joined", id });
        send(r.host, { op: "peer", id });
        log("комната", room, "+ друг", id);
      } else fail(ws, "Неизвестная команда");
      return;
    }
    const r = rooms.get(room);
    if (!r) return;
    const buf = Buffer.isBuffer(data) ? data : Buffer.concat(data);
    if (role === "host") {
      if (buf.length < 4) return;
      const to = buf.readUInt32LE(0);
      const payload = buf.subarray(4);
      if (to === 0) for (const g of r.guests.values()) { if (g.readyState === g.OPEN) g.send(payload); }
      else { const g = r.guests.get(to); if (g && g.readyState === g.OPEN) g.send(payload); }
    } else if (role === "guest") {
      const head = Buffer.alloc(4);
      head.writeUInt32LE(id, 0);
      if (r.host.readyState === r.host.OPEN) r.host.send(Buffer.concat([head, buf]));
    }
  });

  ws.on("close", () => {
    const r = rooms.get(room);
    if (!r) return;
    if (role === "host" && r.host === ws) {
      for (const g of r.guests.values()) fail(g, "Хост закрыл игру");
      rooms.delete(room);
      log("комната", room, "закрыта");
    } else if (role === "guest") {
      r.guests.delete(id);
      send(r.host, { op: "gone", id });
      log("комната", room, "- друг", id);
    }
  });
  ws.on("error", () => {});
});

// Пинг раз в 25 с: держит соединение живым через прокси хостинга и убирает мёртвые.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => log(`relay слушает порт ${PORT}`));
