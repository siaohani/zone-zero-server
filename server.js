const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
const PORT = process.env.PORT || 3001;
const rooms = new Map();
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');
  if (req.url === '/' || req.url === '/health') return res.end(JSON.stringify({ ok: true, game: 'ZONE ZERO BLACKOUT', rooms: rooms.size }));
  res.statusCode = 404; res.end(JSON.stringify({ error: 'not found' }));
});
const wss = new WebSocketServer({ server });
const send = (ws, msg) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); };
function broadcast(room, msg, except) { for (const p of room.players.values()) if (p.ws !== except) send(p.ws, msg); }
function publicPlayer(p) { const { ws, ...out } = p; return out; }
function clamp(v, min, max, fallback = 0) { const n = Number(v); return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback; }
wss.on('connection', ws => {
  ws.id = crypto.randomUUID(); ws.roomCode = null; ws.lastUpdate = 0; ws.lastHit = 0; ws.lastDamageShot = 0;
  ws.on('message', raw => {
    if (raw.length > 8192) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'join') {
      const code = String(m.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      const name = String(m.name || 'Opérateur').replace(/[<>]/g, '').slice(0, 18) || 'Opérateur';
      if (!/^[A-Z0-9]{4,8}$/.test(code)) return send(ws, { type: 'error', message: 'Code de salle invalide.' });
      const requestedMap = String(m.map || 'blacksite').slice(0, 16);
      let r = rooms.get(code); if (!r) { r = { players: new Map(), hostId: null, enemies: new Map(), wave: 1, map: requestedMap }; rooms.set(code, r); }
      if (r.map !== requestedMap) return send(ws, { type: 'error', message: 'Cette salle utilise une autre carte. Choisis la même carte que les autres joueurs.' });
      if (r.players.size >= 12) return send(ws, { type: 'error', message: 'Salle complète.' });
      ws.roomCode = code; ws.playerName = name;
      const isHost = !r.hostId; if (isHost) r.hostId = ws.id;
      const requestedMode = m.mode === 'deathmatch' ? 'deathmatch' : 'coop';
      if (r.mode && r.mode !== requestedMode) return send(ws, { type: 'error', message: 'Cette salle utilise un autre mode de jeu.' });
      r.mode = requestedMode;
      const p = { id: ws.id, name, ws, x: (Math.random()-.5)*24, y: 1.7, z: (Math.random()-.5)*24, yaw: 0, pitch: 0, maxHealth: 100, health: 100, kills: 0, deaths: 0, moving: false, sprinting: false, aiming: false, firing: false, map: requestedMap, mode: requestedMode, isHost };
      if (!r.scores) r.scores = new Map(); r.scores.set(p.id, { id:p.id, name:p.name, kills:0, deaths:0 });
      r.players.set(ws.id, p);
      send(ws, { type: 'joined', id: ws.id, room: code, isHost, mode:r.mode, scores:[...r.scores.values()], players: [...r.players.values()].filter(q => q.id !== ws.id).map(publicPlayer), enemies: r.mode === 'deathmatch' ? [] : [...r.enemies.values()] });
      broadcast(r, { type: 'join', player: publicPlayer(p), scores:[...r.scores.values()] }, ws);
      broadcast(r, { type: 'count', count: r.players.size });
      return;
    }
    if (!ws.roomCode) return;
    const r = rooms.get(ws.roomCode); const p = r?.players.get(ws.id); if (!r || !p) return;
    if (m.type === 'state') {
      const now = Date.now(); if (now - ws.lastUpdate < 50) return; ws.lastUpdate = now;
      p.x = clamp(m.x, -100, 100); p.y = clamp(m.y, -5, 20, 1.7); p.z = clamp(m.z, -100, 100);
      p.yaw = clamp(m.yaw, -20, 20); p.pitch = clamp(m.pitch, -1.6, 1.6);
      p.moving = !!m.moving; p.sprinting = !!m.sprinting; p.aiming = !!m.aiming; p.firing = !!m.firing;
      broadcast(r, { type: 'state', player: publicPlayer(p) }, ws); return;
    }
    if (m.type === 'player-health') {
      p.health = clamp(m.health, 0, p.maxHealth || 100); broadcast(r, { type: 'player-health', id: p.id, health: p.health }, ws);
      broadcast(r, { type: 'state', player: publicPlayer(p) }, ws); return;
    }
    if (m.type === 'score-request') { send(ws, { type:'scoreboard', scores:[...(r.scores?.values() || [])] }); return; }
    if (m.type === 'player-hit') {
      const now = Date.now(); if (now - ws.lastHit < 75) return; ws.lastHit = now;
      const target = r.players.get(String(m.target || '')); if (!target || target.id === p.id || target.health <= 0) return;
      // Fixed server-side damage; client cannot choose arbitrary damage.
      const damage = r.mode === 'deathmatch' ? 9 : clamp(m.damage, 1, 70, 34);
      target.health = Math.max(0, target.health - damage);
      if (target.health <= 0) {
        const killerScore = r.scores.get(p.id); const victimScore = r.scores.get(target.id);
        if (killerScore) killerScore.kills++;
        if (victimScore) victimScore.deaths++;
        send(target.ws, { type: 'player-damaged', target: target.id, attacker: p.id, health: 0, damage });
        send(target.ws, { type: 'player-killed', target: target.id, attacker: p.id });
      } else send(target.ws, { type: 'player-damaged', target: target.id, attacker: p.id, health: target.health, damage });
      broadcast(r, { type: 'state', player: publicPlayer(target) });
      broadcast(r, { type: 'player-hit-confirmed', attacker: p.id, target: target.id, health: target.health, damage });
      broadcast(r, { type:'scoreboard', scores:[...r.scores.values()] }); return;
    }
    if (m.type === 'enemy-state') {
      if (r.mode === 'deathmatch') return;
      if (r.hostId !== ws.id || !Array.isArray(m.enemies)) return;
      r.wave = clamp(m.wave, 1, 999, r.wave);
      const incoming = new Map();
      for (const e of m.enemies.slice(0, 32)) {
        if (!e || typeof e.id !== 'string' || e.id.length > 24) continue;
        const prior = r.enemies.get(e.id);
        const entry = { id: e.id, x: clamp(e.x, -100, 100), y: clamp(e.y, -5, 20), z: clamp(e.z, -100, 100), yaw: clamp(e.yaw, -20, 20), hp: prior ? Math.min(prior.hp, clamp(e.hp, 0, 500)) : clamp(e.hp, 0, 500), maxHp: clamp(e.maxHp, 1, 500, 100), dead: prior ? (prior.dead || !!e.dead) : !!e.dead };
        if (entry.dead) entry.hp = 0;
        incoming.set(entry.id, entry);
      }
      r.enemies = incoming;
      broadcast(r, { type: 'enemy-state', wave: r.wave, enemies: [...r.enemies.values()] }, ws); return;
    }
    if (m.type === 'enemy-hit') {
      if (r.mode === 'deathmatch') return;
      const id = String(m.id || ''); const enemy = r.enemies.get(id); if (!enemy || enemy.dead) return;
      const damage = clamp(m.damage, 1, 70, 34); enemy.hp = Math.max(0, enemy.hp - damage); if (enemy.hp <= 0) enemy.dead = true;
      const result = { type: 'enemy-hit-result', id, hp: enemy.hp, dead: enemy.dead, killer: p.id };
      broadcast(r, result); return;
    }
    if (m.type === 'shot') {
      const now = Date.now();
      // Normalize shot direction outside the rate-limit branch so tracer broadcast never throws.
      const ox = clamp(m.x, -100, 100), oy = clamp(m.y, -5, 20, 1.7), oz = clamp(m.z, -100, 100);
      let dx = clamp(m.dx, -1, 1), dy = clamp(m.dy, -1, 1), dz = clamp(m.dz, -1, 1);
      const norm = Math.hypot(dx, dy, dz) || 1; dx /= norm; dy /= norm; dz /= norm;
      const weapon = ['rifle','smg','sniper'].includes(m.weapon) ? m.weapon : 'rifle';
      const fireGap = weapon === 'smg' ? 75 : weapon === 'sniper' ? 650 : 105;
      if (now - ws.lastDamageShot >= fireGap && m.blocked !== true) {
        ws.lastDamageShot = now;
        if (r.mode === 'deathmatch') {
          let best = null, bestT = Infinity, bestY = 0;
          for (const target of r.players.values()) {
            if (target.id === p.id || target.health <= 0) continue;
            // Player state stores camera/eye position; use a forgiving vertical capsule for the body.
            const cx = target.x - ox, cy = (target.y - 0.72) - oy, cz = target.z - oz;
            const t = cx * dx + cy * dy + cz * dz;
            if (t <= 0 || t > 85 || t >= bestT) continue;
            const ex = cx - dx * t, ey = cy - dy * t, ez = cz - dz * t;
            if (ex * ex + ey * ey + ez * ez <= 1.05 * 1.05) { best = target; bestT = t; bestY = oy + dy * t; }
          }
          if (best) {
            const headshot = bestY > best.y - 0.38;
            const damage = weapon === 'smg' ? 7 : weapon === 'sniper' ? 25 : 9;
            best.health = Math.max(0, best.health - damage);
            if (best.health <= 0) {
              const killerScore = r.scores.get(p.id), victimScore = r.scores.get(best.id);
              if (killerScore) killerScore.kills++;
              if (victimScore) victimScore.deaths++;
              send(best.ws, { type: 'player-damaged', target: best.id, attacker: p.id, health: 0, damage });
              send(best.ws, { type: 'player-killed', target: best.id, attacker: p.id });
            } else send(best.ws, { type: 'player-damaged', target: best.id, attacker: p.id, health: best.health, damage });
            broadcast(r, { type: 'state', player: publicPlayer(best) });
            broadcast(r, { type: 'player-hit-confirmed', attacker: p.id, target: best.id, health: best.health, damage, headshot, weapon });
            broadcast(r, { type: 'scoreboard', scores: [...r.scores.values()] });
          }
        }
      }
      broadcast(r, { type: 'shot', id: ws.id, x: ox, y: oy, z: oz, dx, dy, dz }, ws);
      return;
    }
  });
  ws.on('close', () => {
    if (!ws.roomCode) return; const r = rooms.get(ws.roomCode); if (!r) return;
    r.players.delete(ws.id); r.scores?.delete(ws.id); broadcast(r, { type: 'leave', id: ws.id }); broadcast(r, { type: 'count', count: r.players.size }); broadcast(r, {type:'scoreboard',scores:[...(r.scores?.values()||[])]});
    if (r.hostId === ws.id) {
      r.hostId = r.players.keys().next().value || null;
      if (r.hostId) { const next = r.players.get(r.hostId); next.isHost = true; send(next.ws, { type: 'host-changed', isHost: true }); }
    }
    if (!r.players.size) rooms.delete(ws.roomCode);
  });
});
server.listen(PORT, '0.0.0.0', () => console.log('ZONE ZERO server on ' + PORT));
