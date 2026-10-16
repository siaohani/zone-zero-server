const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
const PORT = process.env.PORT || 3001;
const rooms = new Map();
const serverStartedAt = Date.now();
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');
  if (req.url === '/' || req.url === '/health') {
    let playerCount = 0; for (const room of rooms.values()) playerCount += room.players.size;
    return res.end(JSON.stringify({ ok: true, game: 'ZONE ZERO BLACKOUT', rooms: rooms.size, players: playerCount, uptimeSeconds: Math.floor((Date.now()-serverStartedAt)/1000) }));
  }
  res.statusCode = 404; res.end(JSON.stringify({ error: 'not found' }));
});
// Game messages are small; compression adds CPU and latency for little benefit.
const wss = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 8192 });
const send = (ws, msg) => {
  if (ws.readyState !== WebSocket.OPEN) return;
  // Avoid building an unbounded queue for a slow/proxy-constrained client.
  if (ws.bufferedAmount > 512 * 1024 && ['state','shot','enemy-state'].includes(msg.type)) return;
  ws.send(JSON.stringify(msg));
};
function broadcast(room, msg, except) {
  // Serialize once per room instead of once for every recipient.
  const data = JSON.stringify(msg);
  const volatile = ['state','shot','enemy-state'].includes(msg.type);
  for (const p of room.players.values()) {
    const ws = p.ws; if (ws === except || ws.readyState !== WebSocket.OPEN) continue;
    // Drop obsolete movement/visual updates if a client is falling behind; newer snapshots follow.
    if (volatile && ws.bufferedAmount > 512 * 1024) continue;
    ws.send(data);
  }
}
function publicPlayer(p) { const { ws, ...out } = p; return out; }
function clamp(v, min, max, fallback = 0) { const n = Number(v); return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback; }
function sortedScores(r){const a=[...(r.scores?.values()||[])];if(r.mode==='team-deathmatch'){const totals={red:0,blue:0};for(const x of a)totals[x.team]=(totals[x.team]||0)+(x.kills||0);a.sort((x,y)=>(totals[y.team]||0)-(totals[x.team]||0)||(y.kills||0)-(x.kills||0)||(x.deaths||0)-(y.deaths||0));}else a.sort((x,y)=>(y.kills||0)-(x.kills||0)||(x.deaths||0)-(y.deaths||0));return a;}
function teamCounts(r){const c={red:0,blue:0};for(const p of r.players.values())if(p.team in c)c[p.team]++;return c;}
function startRound(r){if(r.matchTimer||r.matchEndsAt)return;r.matchEndsAt=Date.now()+210000;r.matchTimer=setTimeout(()=>{r.matchFinished=true;const scores=sortedScores(r);for(const p of r.players.values())send(p.ws,{type:'match-end',scores,mode:r.mode,teamScores:r.mode==='team-deathmatch'?{red:scores.filter(x=>x.team==='red').reduce((n,x)=>n+x.kills,0),blue:scores.filter(x=>x.team==='blue').reduce((n,x)=>n+x.kills,0)}:null});},210000);r.matchTimer.unref?.();}
function broadcastTeamCounts(r){broadcast(r,{type:'team-counts',counts:teamCounts(r)});}
wss.on('connection', ws => {
  ws.id = crypto.randomUUID(); ws.roomCode = null; ws.lastUpdate = 0; ws.lastHit = 0; ws.lastDamageShot = 0; ws.isAlive=true; ws.on('pong',()=>{ws.isAlive=true;});
  ws.on('message', raw => {
    if (raw.length > 8192) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'ping') { return send(ws, { type: 'pong', sentAt: Number(m.sentAt) || Date.now() }); }
    if (m.type === 'join') {
      const code = String(m.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      const name = String(m.name || 'Opérateur').replace(/[<>]/g, '').slice(0, 18) || 'Opérateur';
      if (!/^[A-Z0-9]{4,8}$/.test(code)) return send(ws, { type: 'error', message: 'Code de salle invalide.' });
      const requestedMap = String(m.map || 'blacksite').slice(0, 16);
      const allowedModes = ['coop','deathmatch','team-deathmatch','elimination'];
      const requestedMode = allowedModes.includes(m.mode) ? m.mode : 'coop';
      const requestedTeam = m.team === 'blue' ? 'blue' : 'red';
      let r = rooms.get(code);
      if (!r) { r = { players: new Map(), hostId: null, enemies: new Map(), wave: 1, map: requestedMap, mode: requestedMode, scores: new Map(), matchEndsAt:0, matchFinished:false }; rooms.set(code, r); }
      if (r.map !== requestedMap) return send(ws, { type: 'error', message: 'Cette salle utilise une autre carte. Choisis la même carte que les autres joueurs.' });
      if (r.mode !== requestedMode) return send(ws, { type: 'error', message: 'Cette salle utilise un autre mode de jeu.' });
      if (r.matchFinished) return send(ws, { type:'error', message:'Cette manche est terminée. Crée une nouvelle salle avec un autre code.' });
      if (r.players.size >= 12) return send(ws, { type: 'error', message: 'Salle complète.' });
      if (requestedMode === 'team-deathmatch' && teamCounts(r)[requestedTeam] >= 3) return send(ws,{type:'error',message:'Cette équipe est complète. Choisis l’autre équipe.'});
      ws.roomCode = code; ws.playerName = name;
      const isHost = !r.hostId; if (isHost) r.hostId = ws.id;
      let x,z;
      if(requestedMode==='team-deathmatch'){x=requestedTeam==='red'?-42:42;z=(Math.random()-.5)*70;}
      else {const angle=Math.random()*Math.PI*2,dist=38+Math.random()*17;x=Math.cos(angle)*dist;z=Math.sin(angle)*dist;}
      // Avoid spawning too close to another player whenever possible.
      for(let i=0;i<24;i++){if([...r.players.values()].every(q=>Math.hypot(q.x-x,q.z-z)>18))break;if(requestedMode==='team-deathmatch'){x=(requestedTeam==='red'?-1:1)*(35+Math.random()*12);z=(Math.random()-.5)*90;}else{const a=Math.random()*Math.PI*2,d=38+Math.random()*17;x=Math.cos(a)*d;z=Math.sin(a)*d;}}
      const p = { id: ws.id, name, ws, x, y:1.7, z, yaw:0, pitch:0, maxHealth:100, health:100, kills:0, deaths:0, moving:false, sprinting:false, aiming:false, firing:false, map:requestedMap, mode:requestedMode, team:requestedMode==='team-deathmatch'?requestedTeam:null, isHost };
      if (!r.scores) r.scores = new Map(); r.scores.set(p.id, { id:p.id, name:p.name, kills:0, deaths:0, team:p.team });
      r.players.set(ws.id, p); startRound(r);
      send(ws, { type:'joined', id:ws.id, room:code, isHost, mode:r.mode, team:p.team, spawn:{x:p.x,y:p.y,z:p.z}, matchEndsAt:r.matchEndsAt, scores:sortedScores(r), players:[...r.players.values()].filter(q=>q.id!==ws.id).map(publicPlayer), enemies:['deathmatch','team-deathmatch','elimination'].includes(r.mode)?[]:[...r.enemies.values()] });
      broadcast(r, { type:'join', player:publicPlayer(p), scores:sortedScores(r), teamCounts:teamCounts(r) }, ws);
      broadcast(r, { type:'count', count:r.players.size }); broadcastTeamCounts(r);
      for(const player of r.players.values())send(player.ws,{type:'match-tick',endsAt:r.matchEndsAt});
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
      const target = r.players.get(String(m.target || '')); if (!target || target.id === p.id || target.health <= 0 || r.matchFinished) return; if(r.mode==='team-deathmatch'&&target.team===p.team)return;
      // Fixed server-side damage; client cannot choose arbitrary damage.
      const damage = ['deathmatch','team-deathmatch'].includes(r.mode) ? 9 : clamp(m.damage, 1, 70, 34);
      target.health = Math.max(0, target.health - damage);
      if (target.health <= 0) {
        const killerScore = r.scores.get(p.id); const victimScore = r.scores.get(target.id);
        if (killerScore) killerScore.kills++;
        if (victimScore) victimScore.deaths++;
        send(target.ws, { type: 'player-damaged', target: target.id, attacker: p.id, health: 0, damage });
        send(target.ws, { type: 'player-killed', target: target.id, attacker: p.id, attackerName: p.name || 'Opérateur' });
      } else send(target.ws, { type: 'player-damaged', target: target.id, attacker: p.id, health: target.health, damage });
      broadcast(r, { type: 'state', player: publicPlayer(target) });
      broadcast(r, { type: 'player-hit-confirmed', attacker: p.id, target: target.id, health: target.health, damage });
      broadcast(r, { type:'scoreboard', scores:[...r.scores.values()] }); return;
    }
    if (m.type === 'enemy-state') {
      if (['deathmatch','team-deathmatch','elimination'].includes(r.mode)) return;
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
      if (['deathmatch','team-deathmatch','elimination'].includes(r.mode)) return;
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
        if (['deathmatch','team-deathmatch','elimination'].includes(r.mode)) {
          let best = null, bestT = Infinity, bestY = 0;
          for (const target of r.players.values()) {
            if (target.id === p.id || target.health <= 0 || (r.mode==='team-deathmatch'&&target.team===p.team)) continue;
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
              send(best.ws, { type: 'player-killed', target: best.id, attacker: p.id, attackerName: p.name || 'Opérateur' });
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
    r.players.delete(ws.id); r.scores?.delete(ws.id); broadcast(r, { type: 'leave', id: ws.id }); broadcast(r, { type: 'count', count: r.players.size }); broadcast(r, {type:'scoreboard',scores:sortedScores(r)}); broadcastTeamCounts(r);
    if (r.hostId === ws.id) {
      r.hostId = r.players.keys().next().value || null;
      if (r.hostId) { const next = r.players.get(r.hostId); next.isHost = true; send(next.ws, { type: 'host-changed', isHost: true }); }
    }
    if (!r.players.size) { if(r.matchTimer)clearTimeout(r.matchTimer); rooms.delete(ws.roomCode); }
  });
});
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(ws.isAlive===false){ws.terminate();continue;}ws.isAlive=false;ws.ping();}},25000);heartbeat.unref?.();
server.listen(PORT, '0.0.0.0', () => console.log('ZONE ZERO server on ' + PORT));
