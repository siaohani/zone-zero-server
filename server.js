const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
const PORT = process.env.PORT || 3001;
const server = http.createServer((req,res)=>{
  if (req.url === '/health' || req.url === '/') { res.writeHead(200, {'content-type':'application/json','access-control-allow-origin':'*'}); return res.end(JSON.stringify({ok:true, game:'ZONE ZERO BLACKOUT multiplayer', rooms:rooms.size})); }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocketServer({server});
const rooms = new Map();
const send=(ws,data)=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(data));};
function broadcast(room,data,except){for(const p of room.players.values())if(p.ws!==except)send(p.ws,data);}
wss.on('connection',ws=>{
  ws.id=crypto.randomUUID(); ws.roomCode=null; ws.lastUpdate=0;
  send(ws,{type:'welcome',id:ws.id});
  ws.on('message',raw=>{
    if(raw.length>4096)return;
    let m; try{m=JSON.parse(raw.toString())}catch{return}
    if(m.type==='join'){
      const code=String(m.room||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,8);
      const name=String(m.name||'Opérateur').replace(/[<>]/g,'').slice(0,18)||'Opérateur';
      if(!/^[A-Z0-9]{4,8}$/.test(code))return send(ws,{type:'error',message:'Code de salle invalide (4 à 8 lettres/chiffres).'});
      if(ws.roomCode){const old=rooms.get(ws.roomCode);if(old){old.players.delete(ws.id);broadcast(old,{type:'leave',id:ws.id});if(!old.players.size)rooms.delete(ws.roomCode)}}
      let room=rooms.get(code);if(!room){room={players:new Map()};rooms.set(code,room)}
      if(room.players.size>=12)return send(ws,{type:'error',message:'Salle complète (12 joueurs maximum).'});
      ws.roomCode=code;ws.playerName=name;
      const player={id:ws.id,name,ws,x:0,y:0,z:39,yaw:0,pitch:0,health:100};
      room.players.set(ws.id,player);
      send(ws,{type:'joined',id:ws.id,room:code,players:[...room.players.values()].filter(p=>p.id!==ws.id).map(({ws,...p})=>p)});
      broadcast(room,{type:'join',player:{id:player.id,name,x:player.x,y:player.y,z:player.z,yaw:player.yaw,pitch:player.pitch,health:player.health}},ws);
      broadcast(room,{type:'count',count:room.players.size});
    } else if(m.type==='state' && ws.roomCode){
      const now=Date.now(); if(now-ws.lastUpdate<35)return; ws.lastUpdate=now;
      const room=rooms.get(ws.roomCode),p=room?.players.get(ws.id);if(!p)return;
      const num=v=>Number.isFinite(Number(v))?Math.max(-100,Math.min(100,Number(v))):0;
      p.x=num(m.x);p.y=num(m.y);p.z=num(m.z);p.yaw=num(m.yaw);p.pitch=Math.max(-1.6,Math.min(1.6,num(m.pitch)));p.health=Math.max(0,Math.min(100,num(m.health)));
      broadcast(room,{type:'state',player:{id:p.id,name:p.name,x:p.x,y:p.y,z:p.z,yaw:p.yaw,pitch:p.pitch,health:p.health}},ws);
    } else if(m.type==='shot' && ws.roomCode){const room=rooms.get(ws.roomCode);if(room)broadcast(room,{type:'shot',id:ws.id,x:Number(m.x)||0,y:Number(m.y)||0,z:Number(m.z)||0,dx:Number(m.dx)||0,dy:Number(m.dy)||0,dz:Number(m.dz)||0},ws)}
  });
  ws.on('close',()=>{if(!ws.roomCode)return;const room=rooms.get(ws.roomCode);if(!room)return;room.players.delete(ws.id);broadcast(room,{type:'leave',id:ws.id});broadcast(room,{type:'count',count:room.players.size});if(!room.players.size)rooms.delete(ws.roomCode)});
});
server.listen(PORT,'0.0.0.0',()=>console.log(`ZONE ZERO multiplayer server listening on ${PORT}`));
