'use strict';
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
// Collisions des cartes integrees directement au serveur.
// Aucun fichier map-solids.js externe n'est requis.
// Full conservative collider snapshot extracted from this exact client's map-building code.
const MAP_COLLIDER_DATA={"blacksite":[[0,-61,63,1,0,0.9],[0,61,63,1,0,0.9],[-62,0,1,62,0,0.9],[62,0,1,62,0,0.9],[-20,-39,11,11,0,0.9],[20,-39,11,11,0,0.9],[0,-51,22,1,0,0.9],[-14,-27,6,1,0,0.9],[14,-27,6,1,0,0.9],[-27,37,6.15,1.93,0,0.9],[-27,29,6.15,1.93,0,0.9],[27,37,6.15,1.93,0,0.9],[27,29,6.15,1.93,0,0.9],[-40,13,6.15,1.93,0,0.9],[-40,5,6.15,1.93,0,0.9],[40,13,6.15,1.93,0,0.9],[40,5,6.15,1.93,0,0.9],[-25,-7,6.15,1.93,0,0.9],[-25,-15,6.15,1.93,0,0.9],[25,-7,6.15,1.93,0,0.9],[25,-15,6.15,1.93,0,0.9],[-40,-34,6.15,1.93,0,0.9],[40,-34,6.15,1.93,0,0.9],[-28,-49,6.15,1.93,0,0.9],[28,-49,6.15,1.93,0,0.9],[-12,24,3.6,0.48,0,0.9],[12,24,3.6,0.48,0,0.9],[-12,2,3.6,0.48,0,0.9],[12,2,3.6,0.48,0,0.9],[-13,-24,4.1,0.48,0,0.9],[13,-24,4.1,0.48,0,0.9],[-9,-47,3.1,0.48,0,0.9],[9,-47,3.1,0.48,0,0.9],[-18,15,0.9,0.9,0,0.9],[18,15,0.9,0.9,0,0.9],[-18,-9,0.9,0.9,0,0.9],[18,-9,0.9,0.9,0,0.9],[-8,-34,0.9,0.9,0,0.9],[8,-34,0.9,0.9,0,0.9],[-47,25,1.2,1.1,0,0.9],[47,25,1.2,1.1,0,0.9],[-47,-17,1.2,1.1,0,0.9],[47,-17,1.2,1.1,0,0.9],[-16,-54,1.2,1.1,0,0.9],[16,-54,1.2,1.1,0,0.9],[-48,0,1.45,0.95,0,0.9],[48,0,1.45,0.95,0,0.9],[-18,44,1.45,0.95,0,0.9],[18,44,1.45,0.95,0,0.9],[-18,-34,1.45,0.95,0,0.9],[18,-34,1.45,0.95,0,0.9],[-34,3,2.05,0.4,0,0.9],[34,3,2.05,0.4,0,0.9],[-34,-23,2.05,0.4,0,0.9],[34,-23,2.05,0.4,0,0.9],[-8,47,2.05,0.4,0,0.9],[8,47,2.05,0.4,0,0.9],[-62,0,1,62,-0.15,3.25],[62,0,1,62,-0.15,3.25],[-20,-39,11,11,0,8],[20,-39,11,11,0,8],[0,-51,22,1,0,8],[-14,-27,6,1,0,8],[14,-27,6,1,0,8],[-20,-39,0.12,0.12,0.3,7.8],[-15,-39,0.12,0.12,0.3,7.8],[-10,-39,0.12,0.12,0.3,7.8],[-5,-39,0.12,0.12,0.3,7.8],[0,-39,0.12,0.12,0.3,7.8],[5,-39,0.12,0.12,0.3,7.8],[10,-39,0.12,0.12,0.3,7.8],[15,-39,0.12,0.12,0.3,7.8],[20,-39,0.12,0.12,0.3,7.8],[-27,37,6,1.75,0,3.1],[-27,29,6,1.75,0,3.1],[27,37,6,1.75,0,3.1],[27,29,6,1.75,0,3.1],[-40,13,6,1.75,0,3.1],[-40,5,6,1.75,0,3.1],[40,13,6,1.75,0,3.1],[40,5,6,1.75,0,3.1],[-25,-7,6,1.75,0,3.1],[-25,-15,6,1.75,0,3.1],[25,-7,6,1.75,0,3.1],[25,-15,6,1.75,0,3.1],[-40,-34,6,1.75,0,3.1],[40,-34,6,1.75,0,3.1],[-28,-49,6,1.75,0,3.1],[28,-49,6,1.75,0,3.1],[-12,24,3.5,0.38,0,1.05],[12,24,3.5,0.38,0,1.05],[-12,2,3.5,0.38,0,1.05],[12,2,3.5,0.38,0,1.05],[-13,-24,4,0.38,0,1.05],[13,-24,4,0.38,0,1.05],[-9,-47,3,0.38,0,1.05],[9,-47,3,0.38,0,1.05],[-47,-3,0.35,0.35,0,15],[47,-3,0.35,0.35,0,15],[-53,-48,1.4,1.4,0,5],[53,-48,1.4,1.4,0,5],[-53,43,1.4,1.4,0,5],[53,43,1.4,1.4,0,5],[-47,25,1.15,1.05,0,1.7],[47,25,1.15,1.05,0,1.7],[-47,-17,1.15,1.05,0,1.7],[47,-17,1.15,1.05,0,1.7],[-16,-54,1.15,1.05,0,1.7],[16,-54,1.15,1.05,0,1.7],[-49,18,0.85,0.45,0.26,0.78],[49,18,0.85,0.45,0.26,0.78],[-17,-56,0.85,0.45,0.26,0.78],[17,-56,0.85,0.45,0.26,0.78],[-45,-39,0.85,0.45,0.26,0.78],[45,-39,0.85,0.45,0.26,0.78],[-54.85,-34,0.21,0.16,1.73,2.28],[-54.85,-30,0.21,0.16,1.73,2.28],[-54.85,-26,0.21,0.16,1.73,2.28],[-54.85,-22,0.21,0.16,1.73,2.28],[-54.85,-18,0.21,0.16,1.73,2.28],[-54.85,-14,0.21,0.16,1.73,2.28],[-54.85,-10,0.21,0.16,1.73,2.28],[-54.85,-6,0.21,0.16,1.73,2.28],[-54.85,-2,0.21,0.16,1.73,2.28],[-54.85,2,0.21,0.16,1.73,2.28],[-54.85,6,0.21,0.16,1.73,2.28],[-54.85,10,0.21,0.16,1.73,2.28],[-54.85,14,0.21,0.16,1.73,2.28],[-54.85,18,0.21,0.16,1.73,2.28],[-54.85,22,0.21,0.16,1.73,2.28],[-54.85,26,0.21,0.16,1.73,2.28],[-54.85,30,0.21,0.16,1.73,2.28],[-54.85,34,0.21,0.16,1.73,2.28],[54.85,-34,0.21,0.16,1.73,2.28],[54.85,-30,0.21,0.16,1.73,2.28],[54.85,-26,0.21,0.16,1.73,2.28],[54.85,-22,0.21,0.16,1.73,2.28],[54.85,-18,0.21,0.16,1.73,2.28],[54.85,-14,0.21,0.16,1.73,2.28],[54.85,-10,0.21,0.16,1.73,2.28],[54.85,-6,0.21,0.16,1.73,2.28],[54.85,-2,0.21,0.16,1.73,2.28],[54.85,2,0.21,0.16,1.73,2.28],[54.85,6,0.21,0.16,1.73,2.28],[54.85,10,0.21,0.16,1.73,2.28],[54.85,14,0.21,0.16,1.73,2.28],[54.85,18,0.21,0.16,1.73,2.28],[54.85,22,0.21,0.16,1.73,2.28],[54.85,26,0.21,0.16,1.73,2.28],[54.85,30,0.21,0.16,1.73,2.28],[54.85,34,0.21,0.16,1.73,2.28],[-48,23,0.68,0.63,0.06,1.14],[-47.82,23,0.85,0.63,1.28,2.36],[48,23,0.68,0.63,0.06,1.14],[48.18,23,0.85,0.63,1.28,2.36],[-48,-20,0.68,0.63,0.06,1.14],[-47.82,-20,0.85,0.63,1.28,2.36],[48,-20,0.68,0.63,0.06,1.14],[48.18,-20,0.85,0.63,1.28,2.36],[-19,-55,0.68,0.63,0.06,1.14],[-18.82,-55,0.85,0.63,1.28,2.36],[19,-55,0.68,0.63,0.06,1.14],[19.18,-55,0.85,0.63,1.28,2.36],[-35,19,0.68,0.63,0.06,1.14],[-34.82,19,0.85,0.63,1.28,2.36],[35,19,0.68,0.63,0.06,1.14],[35.18,19,0.85,0.63,1.28,2.36],[-56,32.1,0.15,0.15,0,1.1],[-56,28.1,0.15,0.15,0,1.1],[56,32.1,0.15,0.15,0,1.1],[56,28.1,0.15,0.15,0,1.1],[-56,-14.9,0.15,0.15,0,1.1],[56,-14.9,0.15,0.15,0,1.1],[-47,-8,0.86,0.86,3.2,11.2],[-47,2,0.86,0.86,3.2,11.2],[47,-8,0.86,0.86,3.2,11.2],[47,2,0.86,0.86,3.2,11.2],[-39,34,0.53,0.5,0.08,1.08],[-38.84,34,0.65,0.5,1.13,2.13],[39,34,0.53,0.5,0.08,1.08],[39.16,34,0.65,0.5,1.13,2.13],[-39,-8,0.53,0.5,0.08,1.08],[-38.84,-8,0.65,0.5,1.13,2.13],[39,-8,0.53,0.5,0.08,1.08],[39.16,-8,0.65,0.5,1.13,2.13],[-27,17,0.53,0.5,0.08,1.08],[-26.84,17,0.65,0.5,1.13,2.13],[27,17,0.53,0.5,0.08,1.08],[27.16,17,0.65,0.5,1.13,2.13],[-27,-18,0.53,0.5,0.08,1.08],[-26.84,-18,0.65,0.5,1.13,2.13],[27,-18,0.53,0.5,0.08,1.08],[27.16,-18,0.65,0.5,1.13,2.13],[-52,8,0.43,0.36,0,1.1],[52,8,0.43,0.36,0,1.1],[-52,-27,0.43,0.36,0,1.1],[52,-27,0.43,0.36,0,1.1],[-34,45,0.43,0.36,0,1.1],[34,45,0.43,0.36,0,1.1],[-48.52,-0.31,0.46,0.28,0.14,0.94],[-47.47,-0.31,0.46,0.28,0.14,0.94],[-48.52,0.31,0.46,0.28,0.14,0.86],[-47.47,0.31,0.46,0.28,0.14,0.86],[47.48,-0.31,0.46,0.28,0.14,0.94],[48.53,-0.31,0.46,0.28,0.14,0.94],[47.48,0.31,0.46,0.28,0.14,0.86],[48.53,0.31,0.46,0.28,0.14,0.86],[-18.52,43.69,0.46,0.28,0.14,0.94],[-17.48,43.69,0.46,0.28,0.14,0.94],[-18.52,44.31,0.46,0.28,0.14,0.86],[-17.48,44.31,0.46,0.28,0.14,0.86],[17.48,43.69,0.46,0.28,0.14,0.94],[18.52,43.69,0.46,0.28,0.14,0.94],[17.48,44.31,0.46,0.28,0.14,0.86],[18.52,44.31,0.46,0.28,0.14,0.86],[-18.52,-34.31,0.46,0.28,0.14,0.94],[-17.48,-34.31,0.46,0.28,0.14,0.94],[-18.52,-33.69,0.46,0.28,0.14,0.86],[-17.48,-33.69,0.46,0.28,0.14,0.86],[17.48,-34.31,0.46,0.28,0.14,0.94],[18.52,-34.31,0.46,0.28,0.14,0.94],[17.48,-33.69,0.46,0.28,0.14,0.86],[18.52,-33.69,0.46,0.28,0.14,0.86],[-34,3,2,0.33,0,1],[34,3,2,0.33,0,1],[-34,-23,2,0.33,0,1],[34,-23,2,0.33,0,1],[-8,47,2,0.33,0,1],[8,47,2,0.33,0,1]],"shipment":[[0,-28,29,0.5,0,0.9],[0,28,29,0.5,0,0.9],[-28,0,0.5,29,0,0.9],[28,0,0.5,29,0,0.9],[-8,-18,6.12,1.92,0,0.9],[8,-18,6.12,1.92,0,0.9],[-19,-5,1.92,5.62,0,0.9],[19,5,1.92,5.62,0,0.9],[0,0,6.12,1.92,0,0.9],[-8,18,6.12,1.92,0,0.9],[8,18,6.12,1.92,0,0.9],[-19,16,1.92,3.37,0,0.9],[19,-16,1.92,3.37,0,0.9],[-8,-8,1.75,0.6,0,0.9],[8,8,1.75,0.6,0,0.9],[-8,8,1.75,0.6,0,0.9],[8,-8,1.75,0.6,0,0.9],[0,-28,29,0.5,0,3.6],[0,28,29,0.5,0,3.6],[-28,0,0.5,29,0,3.6],[28,0,0.5,29,0,3.6],[-8,-18,6,1.8,0,3.15],[8,-18,6,1.8,0,3.15],[-19,-5,1.8,5.5,0,3.15],[19,5,1.8,5.5,0,3.15],[0,0,6,1.8,0,3.15],[-8,18,6,1.8,0,3.15],[8,18,6,1.8,0,3.15],[-19,16,1.8,3.25,0,3.15],[19,-16,1.8,3.25,0,3.15],[-8,-8,1.7,0.55,0,0.92],[8,8,1.7,0.55,0,0.92],[-8,8,1.7,0.55,0,0.92],[8,-8,1.7,0.55,0,0.92]],"arctic":[[0,-61,62,1,0,0.9],[0,61,62,1,0,0.9],[-61,0,1,62,0,0.9],[61,0,1,62,0,0.9],[-24,-35,9,1,0,0.9],[-33,-25,1,10,0,0.9],[-15,-25,1,10,0,0.9],[-29.75,-15,3.25,1,0,0.9],[-18.25,-15,3.25,1,0,0.9],[25,-34,9,1,0,0.9],[16,-25,1,9,0,0.9],[34,-25,1,9,0,0.9],[19.25,-16,3.25,1,0,0.9],[30.75,-16,3.25,1,0,0.9],[-43,17,4.5,1.5,0,0.9],[43,17,4.5,1.5,0,0.9],[-19,12,3.5,1,0,0.9],[19,12,3.5,1,0,0.9],[-42,-5,3.5,1,0,0.9],[42,-5,3.5,1,0,0.9],[0,-4,4.5,1,0,0.9],[-47,42,1.2,1.15,0,0.9],[47,42,1.2,1.15,0,0.9],[-45,-47,1.2,1.15,0,0.9],[45,-47,1.2,1.15,0,0.9],[-5,-48,1.2,1.15,0,0.9],[6,38,1.2,1.15,0,0.9],[-29,-29,0.58,0.5,0,0.9],[-19,-29,0.58,0.5,0,0.9],[-29,-22,0.58,0.5,0,0.9],[-19,-22,0.58,0.5,0,0.9],[20,-29,0.58,0.5,0,0.9],[30,-29,0.58,0.5,0,0.9],[20,-22,0.58,0.5,0,0.9],[30,-22,0.58,0.5,0,0.9],[-48,34,0.58,0.5,0,0.9],[48,34,0.58,0.5,0,0.9],[-48,-8,0.58,0.5,0,0.9],[48,-8,0.58,0.5,0,0.9],[-43,30,0.5,0.5,0,0.9],[43,30,0.5,0.5,0,0.9],[-43,-12,0.5,0.5,0,0.9],[43,-12,0.5,0.5,0,0.9],[-30,18,1.55,1,0,0.9],[30,18,1.55,1,0,0.9],[-30,-3,1.55,1,0,0.9],[30,-3,1.55,1,0,0.9],[-30,-43,1.55,1,0,0.9],[30,-43,1.55,1,0,0.9],[-47,8,1.55,1,0,0.9],[47,8,1.55,1,0,0.9],[-43,1,2.55,0.38,0,0.9],[43,1,2.55,0.38,0,0.9],[-22,27,2.05,0.38,0,0.9],[22,27,2.05,0.38,0,0.9],[-22,-42,2.05,0.38,0,0.9],[22,-42,2.05,0.38,0,0.9],[0,-61,62,1,-0.15,3.35],[0,61,62,1,-0.15,3.35],[-61,0,1,62,-0.15,3.35],[61,0,1,62,-0.15,3.35],[-24,-35,9,1,0,7],[-33,-25,1,10,0,7],[-15,-25,1,10,0,7],[-29.75,-15,3.25,1,0,7],[-18.25,-15,3.25,1,0,7],[25,-34,9,1,0,5],[16,-25,1,9,0,5],[34,-25,1,9,0,5],[19.25,-16,3.25,1,0,5],[30.75,-16,3.25,1,0,5],[-43,17,4.5,1.5,0,1.3],[43,17,4.5,1.5,0,1.3],[-19,12,3.5,1,0,1.3],[19,12,3.5,1,0,1.3],[-42,-5,3.5,1,0,1.3],[42,-5,3.5,1,0,1.3],[0,-4,4.5,1,0,1.3],[-47,42,1.1,1.05,0,1.7],[47,42,1.1,1.05,0,1.7],[-45,-47,1.1,1.05,0,1.7],[45,-47,1.1,1.05,0,1.7],[-5,-48,1.1,1.05,0,1.7],[6,38,1.1,1.05,0,1.7],[-35,-3,0.11,0.11,0,12],[-5,-3,0.11,0.11,0,12],[25,-3,0.11,0.11,0,12],[-48,28,1.25,0.85,0,1.5],[48,28,1.25,0.85,0,1.5],[-47,-15,1.25,0.85,0,1.5],[47,-15,1.25,0.85,0,1.5],[-8,-51,1.25,0.85,0,1.5],[8,-51,1.25,0.85,0,1.5],[-48.42,-44,0.61,0.61,1.8,7.6],[-47.58,-44,0.61,0.61,1.8,7.6],[47.58,-44,0.61,0.61,1.8,7.6],[48.42,-44,0.61,0.61,1.8,7.6],[-48.42,45,0.61,0.61,1.8,7.6],[-47.58,45,0.61,0.61,1.8,7.6],[47.58,45,0.61,0.61,1.8,7.6],[48.42,45,0.61,0.61,1.8,7.6],[-0.42,-53,0.61,0.61,1.8,7.6],[0.42,-53,0.61,0.61,1.8,7.6],[-29,-29,0.53,0.46,0,1],[-19,-29,0.53,0.46,0,1],[-29,-22,0.53,0.46,0,1],[-19,-22,0.53,0.46,0,1],[20,-29,0.53,0.46,0,1],[30,-29,0.53,0.46,0,1],[20,-22,0.53,0.46,0,1],[30,-22,0.53,0.46,0,1],[-48,34,0.53,0.46,0,1],[48,34,0.53,0.46,0,1],[-48,-8,0.53,0.46,0,1],[48,-8,0.53,0.46,0,1],[-44,20,0.12,6,0.3,0.54],[44,20,0.12,6,0.3,0.54],[-44,-21,0.12,4.5,0.3,0.54],[44,-21,0.12,4.5,0.3,0.54],[-32.65,-34.08,0.11,0.11,0,7],[-15.35,-34.08,0.11,0.11,0,7],[16.35,-33.08,0.11,0.11,0,5],[33.65,-33.08,0.11,0.11,0,5],[-48.7,-44,0.77,0.77,0.55,8.05],[-47.3,-44,0.77,0.77,0.55,8.05],[-49.2,-43.1,0.35,0.3,0.01,1.06],[47.3,-44,0.77,0.77,0.55,8.05],[48.7,-44,0.77,0.77,0.55,8.05],[46.8,-43.1,0.35,0.3,0.01,1.06],[-48.7,45,0.77,0.77,0.55,8.05],[-47.3,45,0.77,0.77,0.55,8.05],[-49.2,45.9,0.35,0.3,0.01,1.06],[47.3,45,0.77,0.77,0.55,8.05],[48.7,45,0.77,0.77,0.55,8.05],[46.8,45.9,0.35,0.3,0.01,1.06],[-0.7,-53,0.77,0.77,0.55,8.05],[0.7,-53,0.77,0.77,0.55,8.05],[-1.2,-52.1,0.35,0.3,0.01,1.06],[-34,12,0.9,0.5,0.24,0.92],[34,12,0.9,0.5,0.24,0.92],[-46,-27,0.9,0.5,0.24,0.92],[46,-27,0.9,0.5,0.24,0.92],[-14,38,0.9,0.5,0.24,0.92],[14,-39,0.9,0.5,0.24,0.92],[-34,5,0.56,0.45,0.07,0.89],[-33.87,5,0.67,0.45,0.97,1.79],[-35.3,6.1,0.11,0.11,0,0.72],[-32.7,6.1,0.11,0.11,0,0.72],[34,5,0.56,0.45,0.07,0.89],[34.13,5,0.67,0.45,0.97,1.79],[32.7,6.1,0.11,0.11,0,0.72],[35.3,6.1,0.11,0.11,0,0.72],[-34,34,0.56,0.45,0.07,0.89],[-33.87,34,0.67,0.45,0.97,1.79],[-35.3,35.1,0.11,0.11,0,0.72],[-32.7,35.1,0.11,0.11,0,0.72],[34,34,0.56,0.45,0.07,0.89],[34.13,34,0.67,0.45,0.97,1.79],[32.7,35.1,0.11,0.11,0,0.72],[35.3,35.1,0.11,0.11,0,0.72],[-50,1,0.56,0.45,0.07,0.89],[-49.87,1,0.67,0.45,0.97,1.79],[-51.3,2.1,0.11,0.11,0,0.72],[-48.7,2.1,0.11,0.11,0,0.72],[50,1,0.56,0.45,0.07,0.89],[50.13,1,0.67,0.45,0.97,1.79],[48.7,2.1,0.11,0.11,0,0.72],[51.3,2.1,0.11,0.11,0,0.72],[-50,-35,0.56,0.45,0.07,0.89],[-49.87,-35,0.67,0.45,0.97,1.79],[-51.3,-33.9,0.11,0.11,0,0.72],[-48.7,-33.9,0.11,0.11,0,0.72],[50,-35,0.56,0.45,0.07,0.89],[50.13,-35,0.67,0.45,0.97,1.79],[48.7,-33.9,0.11,0.11,0,0.72],[51.3,-33.9,0.11,0.11,0,0.72],[-8,39,0.56,0.45,0.07,0.89],[-7.87,39,0.67,0.45,0.97,1.79],[-9.3,40.1,0.11,0.11,0,0.72],[-6.7,40.1,0.11,0.11,0,0.72],[8,39,0.56,0.45,0.07,0.89],[8.13,39,0.67,0.45,0.97,1.79],[6.7,40.1,0.11,0.11,0,0.72],[9.3,40.1,0.11,0.11,0,0.72],[-54,22,0.6,0.4,0,0.88],[54,22,0.6,0.4,0,0.88],[-54,-9,0.6,0.4,0,0.88],[54,-9,0.6,0.4,0,0.88],[-28,42,0.6,0.4,0,0.88],[28,42,0.6,0.4,0,0.88],[-28,-46,0.6,0.4,0,0.88],[28,-46,0.6,0.4,0,0.88],[-30.56,17.69,0.49,0.28,0.14,0.96],[-29.44,17.69,0.49,0.28,0.14,0.96],[-30.56,18.31,0.49,0.28,0.14,0.9],[-29.44,18.31,0.49,0.28,0.14,0.9],[29.44,17.69,0.49,0.28,0.14,0.96],[30.56,17.69,0.49,0.28,0.14,0.96],[29.44,18.31,0.49,0.28,0.14,0.9],[30.56,18.31,0.49,0.28,0.14,0.9],[-30.56,-3.31,0.49,0.28,0.14,0.96],[-29.44,-3.31,0.49,0.28,0.14,0.96],[-30.56,-2.69,0.49,0.28,0.14,0.9],[-29.44,-2.69,0.49,0.28,0.14,0.9],[29.44,-3.31,0.49,0.28,0.14,0.96],[30.56,-3.31,0.49,0.28,0.14,0.96],[29.44,-2.69,0.49,0.28,0.14,0.9],[30.56,-2.69,0.49,0.28,0.14,0.9],[-30.56,-43.31,0.49,0.28,0.14,0.96],[-29.44,-43.31,0.49,0.28,0.14,0.96],[-30.56,-42.69,0.49,0.28,0.14,0.9],[-29.44,-42.69,0.49,0.28,0.14,0.9],[29.44,-43.31,0.49,0.28,0.14,0.96],[30.56,-43.31,0.49,0.28,0.14,0.96],[29.44,-42.69,0.49,0.28,0.14,0.9],[30.56,-42.69,0.49,0.28,0.14,0.9],[-47.56,7.69,0.49,0.28,0.14,0.96],[-46.44,7.69,0.49,0.28,0.14,0.96],[-47.56,8.31,0.49,0.28,0.14,0.9],[-46.44,8.31,0.49,0.28,0.14,0.9],[46.44,7.69,0.49,0.28,0.14,0.96],[47.56,7.69,0.49,0.28,0.14,0.96],[46.44,8.31,0.49,0.28,0.14,0.9],[47.56,8.31,0.49,0.28,0.14,0.9],[-43,1,2.5,0.33,0,0.92],[43,1,2.5,0.33,0,0.92],[-22,27,2,0.33,0,0.92],[22,27,2,0.33,0,0.92],[-22,-42,2,0.33,0,0.92],[22,-42,2,0.33,0,0.92]]};
function mapSolids(map){return (MAP_COLLIDER_DATA[map]||[]).map(([x,z,hx,hz,minY,maxY])=>({x,z,hx,hz,minY,maxY}));}
// Ray vs 3D AABB using slabs. Ignore intersections beyond the target.
const mapCache=new Map();
function spawnBlocked(map,x,z,radius=.72){
 const solids=mapSolids(map);
 return solids.some(c=>x>c.x-c.hx-radius&&x<c.x+c.hx+radius&&z>c.z-c.hz-radius&&z<c.z+c.hz+radius&&c.maxY>0.05);
}
function chooseRespawn(room,player,request={}){
 const limit=room.map==='shipment'?26.5:58.5;
 const oldX=Number.isFinite(player.deadX)?player.deadX:player.x;
 const oldZ=Number.isFinite(player.deadZ)?player.deadZ:player.z;
 const teamSide=player.mode==='team-deathmatch'?(player.team==='red'?-1:1):0;
 const safe=(x,z)=>Number.isFinite(x)&&Number.isFinite(z)&&Math.abs(x)<=limit&&Math.abs(z)<=limit&&(!teamSide||Math.sign(x)===teamSide)&&!spawnBlocked(room.map,x,z,.85)&&
   Math.hypot(x-oldX,z-oldZ)>=8&&
   [...room.players.values()].every(q=>q.id===player.id||q.health<=0||Math.hypot(x-q.x,z-q.z)>=5);
 // The client checks the complete rendered collision geometry; prefer its candidate
 // when it also passes server bounds, known-solid and player-distance checks.
 const requestedX=Number(request.x),requestedZ=Number(request.z);
 if(safe(requestedX,requestedZ))return {x:requestedX,z:requestedZ};
 for(let i=0;i<500;i++){
   let x,z;
   if(room.map==='shipment'){
     x=(Math.random()<.5?-1:1)*(8+Math.random()*16);z=(Math.random()*2-1)*24;
   }else if(teamSide){x=teamSide*(24+Math.random()*31);z=(Math.random()*2-1)*55;}
   else{x=(Math.random()*2-1)*55;z=(Math.random()*2-1)*55;}
   if(safe(x,z))return {x,z};
 }
 // Deterministic fallback also checks collisions and distance; never knowingly spawn inside cover.
 for(let x=-Math.floor(limit);x<=Math.floor(limit);x+=3)for(let z=-Math.floor(limit);z<=Math.floor(limit);z+=3){
   if(teamSide&&Math.sign(x)!==teamSide)continue;
   if(safe(x,z))return {x,z};
 }
 // Under extreme crowding, ignore proximity to living players but never ignore
 // map collision or the minimum distance from the last death location.
 for(let x=-Math.floor(limit);x<=Math.floor(limit);x+=2)for(let z=-Math.floor(limit);z<=Math.floor(limit);z+=2){
   if(teamSide&&Math.sign(x)!==teamSide)continue;
   if(!spawnBlocked(room.map,x,z,.85)&&Math.hypot(x-oldX,z-oldZ)>=8)return {x,z};
 }
 // No valid point: keep the player dead and let the client retry; never emit an unsafe spawn.
 return null;
}
function coverBefore(map,origin,dir,distance){
 let solids=mapCache.get(map);if(!solids){solids=mapSolids(map);mapCache.set(map,solids);}
 for(const c of solids){
  let tmin=0,tmax=distance,valid=true;
  const low=[c.x-c.hx,c.minY,c.z-c.hz],hi=[c.x+c.hx,c.maxY,c.z+c.hz];
  const start=[origin.x,origin.y,origin.z],v=[dir.x,dir.y,dir.z];
  for(let i=0;i<3;i++){
   if(Math.abs(v[i])<1e-9){if(start[i]<low[i]||start[i]>hi[i]){valid=false;break;}}
   else{let a=(low[i]-start[i])/v[i],b=(hi[i]-start[i])/v[i];if(a>b)[a,b]=[b,a];tmin=Math.max(tmin,a);tmax=Math.min(tmax,b);if(tmin>tmax){valid=false;break;}}
  }
  if(valid&&tmax>=0&&tmin<distance-.06)return true;
 }
 return false;
}

// V46.7: targeted, bounded lag compensation for moving players. No client-supplied
// "hit" or timing is trusted. All impact and cover decisions remain server-side.
const PLAYER_HISTORY_MS=270;
function rememberPlayerPosition(p,now=Date.now()) {
  if (!p.history) p.history=[];
  p.history.push({x:p.x,y:p.y,z:p.z,crouching:!!p.crouching,at:now});
  while (p.history.length>1 && p.history[0].at<now-PLAYER_HISTORY_MS) p.history.shift();
  if(p.history.length>9)p.history.splice(0,p.history.length-9);
}
function raySphereT(o,d,cx,cy,cz,r,maxT) {
  const x=o.x-cx,y=o.y-cy,z=o.z-cz;
  const b=x*d.x+y*d.y+z*d.z;
  const c=x*x+y*y+z*z-r*r;
  const det=b*b-c;if(det<0)return Infinity;
  const root=Math.sqrt(det),t=-b-root;
  const t2=t>=0?t:-b+root;
  return t2>=0&&t2<=maxT?t2:Infinity;
}
// Torso + limbs, modeled as a vertical finite cylinder. For a near-vertical
// shot, test its caps too (otherwise shooting down at a player misses).
function rayCylinderT(o,d,cx,cz,minY,maxY,r,maxT) {
  const rx=o.x-cx,rz=o.z-cz,a=d.x*d.x+d.z*d.z;
  let best=Infinity;
  if(a>1e-9){
    const b=rx*d.x+rz*d.z,c=rx*rx+rz*rz-r*r,disc=b*b-a*c;
    if(disc>=0){const root=Math.sqrt(disc);
      for(const t of [(-b-root)/a,(-b+root)/a]){
        if(t>=0&&t<best&&t<=maxT){const y=o.y+t*d.y;if(y>=minY&&y<=maxY)best=t;}
      }
    }
  }
  if(Math.abs(d.y)>1e-9)for(const y of [minY,maxY]){
    const t=(y-o.y)/d.y;
    if(t>=0&&t<best&&t<=maxT){const x=rx+d.x*t,z=rz+d.z*t;if(x*x+z*z<=r*r)best=t;}
  }
  // Firing from inside the hitbox must also register.
  if(o.y>=minY&&o.y<=maxY&&rx*rx+rz*rz<=r*r)best=0;
  return best;
}
function playerRayImpact(o,d,target,maxT=85){
  const crouched=!!target.crouching;
  const eye=target.y,foot=eye-(crouched?1.05:1.7);
  const head=raySphereT(o,d,target.x,eye+.015,target.z,.285,maxT);
  const body=rayCylinderT(o,d,target.x,target.z,foot+.17,eye-.245,.49,maxT);
  if(!Number.isFinite(head)&&!Number.isFinite(body))return null;
  // If head and chest overlap slightly, prefer the genuinely earlier intersection.
  return head<=body?{t:head,headshot:true}:{t:body,headshot:false};
}
function playerShotCandidates(target,now,rewindMs){
  // Network smoothing in the browser introduces ~50-80 ms of visual delay,
  // on top of one-way network latency. Search only a small history window.
  const states=target.history||[];
  const near=states.filter(h=>now-h.at>=0&&now-h.at<=PLAYER_HISTORY_MS&&
    Math.abs((now-h.at)-rewindMs)<=70);
  if(!near.length&&states.length){
    const closest=[...states].filter(h=>now-h.at>=0&&now-h.at<=PLAYER_HISTORY_MS)
      .sort((a,b)=>Math.abs(now-a.at-rewindMs)-Math.abs(now-b.at-rewindMs))[0];
    if(closest)near.push(closest);
  }
  // Always allow the authoritative current location to be hit as well.
  return [target,...near];
}
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
function publicPlayer(p) { const { ws, history, ...out } = p; return out; }
function clamp(v, min, max, fallback = 0) { const n = Number(v); return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback; }
function sortedScores(r){const a=[...(r.scores?.values()||[])];if(r.mode==='team-deathmatch'){const totals={red:0,blue:0};for(const x of a)totals[x.team]=(totals[x.team]||0)+(x.kills||0);a.sort((x,y)=>(totals[y.team]||0)-(totals[x.team]||0)||(y.kills||0)-(x.kills||0)||(x.deaths||0)-(y.deaths||0));}else a.sort((x,y)=>(y.kills||0)-(x.kills||0)||(x.deaths||0)-(y.deaths||0));return a;}
function teamCounts(r){const c={red:0,blue:0};for(const p of r.players.values())if(p.team in c)c[p.team]++;return c;}
function startRound(r){if(r.matchTimer||r.matchEndsAt)return;r.roundStartsAt=Date.now()+5000;r.matchEndsAt=r.roundStartsAt+210000;for(const p of r.players.values())send(p.ws,{type:'match-tick',startsAt:r.roundStartsAt,endsAt:r.matchEndsAt});r.matchTimer=setTimeout(()=>{r.matchStarted=true;for(const p of r.players.values())send(p.ws,{type:'round-start',startsAt:r.roundStartsAt,endsAt:r.matchEndsAt});r.matchEndTimer=setTimeout(()=>{r.matchFinished=true;const scores=sortedScores(r);for(const p of r.players.values())send(p.ws,{type:'match-end',scores,mode:r.mode,teamScores:r.mode==='team-deathmatch'?{red:scores.filter(x=>x.team==='red').reduce((n,x)=>n+x.kills,0),blue:scores.filter(x=>x.team==='blue').reduce((n,x)=>n+x.kills,0)}:null});},210000);r.matchEndTimer.unref?.();},5000);r.matchTimer.unref?.();}
function broadcastTeamCounts(r){broadcast(r,{type:'team-counts',counts:teamCounts(r)});}
wss.on('connection', ws => {
  ws.id = crypto.randomUUID(); ws.roomCode = null; ws.lastUpdate = 0; ws.lastHit = 0; ws.lastDamageShot = 0; ws.isAlive=true;ws.roundTripMs=120;ws.lastWsPingAt=0; ws.on('pong',()=>{ws.isAlive=true;if(ws.lastWsPingAt){ws.roundTripMs=clamp(Date.now()-ws.lastWsPingAt,10,400,120);ws.lastWsPingAt=0;}});
  ws.on('message', raw => {
    if (raw.length > 8192) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    // JSON.parse accepts null and primitives; never let a malformed client packet crash Node.
    if (!m || typeof m !== 'object' || Array.isArray(m) || typeof m.type !== 'string') return;
    if (m.type === 'ping') { return send(ws, { type: 'pong', sentAt: Number(m.sentAt) || Date.now() }); }
    if (m.type === 'admin-auth') { const configured=String(process.env.ADMIN_CODE||''); const supplied=String(m.code||'').slice(0,32); ws.isAdmin=!!configured&&supplied===configured; return send(ws,{type:'admin-auth-result',ok:ws.isAdmin}); }
    if (m.type === 'admin-disconnect-all') { if(!ws.isAdmin)return send(ws,{type:'admin-disconnect-result',ok:false}); const others=[...wss.clients].filter(client=>client!==ws&&client.readyState===WebSocket.OPEN); for(const client of others)send(client,{type:'server-reset',message:'Toutes les parties ont été arrêtées par l’administrateur. Retour au menu.'}); for(const r of rooms.values()){if(r.matchTimer)clearTimeout(r.matchTimer);if(r.matchEndTimer)clearTimeout(r.matchEndTimer);} rooms.clear(); for(const client of others){client.roomCode=null;} send(ws,{type:'admin-disconnect-result',ok:true}); setTimeout(()=>{for(const client of others){try{client.close(4001,'Admin reset');}catch(_){}}},900).unref?.(); return; }
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
      if (r.players.size >= 32) return send(ws, { type: 'error', message: 'Salle complète (32 joueurs maximum).' });
      ws.roomCode = code; ws.playerName = name;
      const isHost = !r.hostId; if (isHost) r.hostId = ws.id;
      let x,z;
      if(requestedMap==='shipment'){
        if(requestedMode==='team-deathmatch'){x=(requestedTeam==='red'?-1:1)*22;z=(Math.random()-.5)*8;}
        else {x=(Math.random()<.5?-1:1)*(20+Math.random()*2);z=(Math.random()<.5?-1:1)*(20+Math.random()*2);}
      }else if(requestedMode==='team-deathmatch'){x=requestedTeam==='red'?-42:42;z=(Math.random()-.5)*70;}
      else {const angle=Math.random()*Math.PI*2,dist=38+Math.random()*17;x=Math.cos(angle)*dist;z=Math.sin(angle)*dist;}
      // Avoid spawning too close to another player whenever possible.
      for(let i=0;i<24;i++){
        if([...r.players.values()].every(q=>Math.hypot(q.x-x,q.z-z)>(requestedMap==='shipment'?7:18)))break;
        if(requestedMap==='shipment'){
          if(requestedMode==='team-deathmatch'){x=(requestedTeam==='red'?-1:1)*22;z=(Math.random()-.5)*8;}
          else {x=(Math.random()<.5?-1:1)*(20+Math.random()*2);z=(Math.random()<.5?-1:1)*(20+Math.random()*2);}
        }else if(requestedMode==='team-deathmatch'){x=(requestedTeam==='red'?-1:1)*(35+Math.random()*12);z=(Math.random()-.5)*90;}
        else{const a=Math.random()*Math.PI*2,d=38+Math.random()*17;x=Math.cos(a)*d;z=Math.sin(a)*d;}
      }
      const p = { id: ws.id, name, ws, x, y:1.7, z, yaw:0, pitch:0, maxHealth:100, health:100, kills:0, deaths:0, moving:false, sprinting:false, aiming:false, firing:false, crouching:false, history:[], map:requestedMap, mode:requestedMode, team:requestedMode==='team-deathmatch'?requestedTeam:null, isHost };
      if (!r.scores) r.scores = new Map(); r.scores.set(p.id, { id:p.id, name:p.name, kills:0, deaths:0, team:p.team });
      rememberPlayerPosition(p);r.players.set(ws.id, p); startRound(r);
      send(ws, { type:'joined', id:ws.id, room:code, isHost, mode:r.mode, team:p.team, spawn:{x:p.x,y:p.y,z:p.z}, matchEndsAt:r.matchEndsAt, roundStartsAt:r.roundStartsAt, scores:sortedScores(r), players:[...r.players.values()].filter(q=>q.id!==ws.id).map(publicPlayer), enemies:['deathmatch','team-deathmatch','elimination'].includes(r.mode)?[]:[...r.enemies.values()] });
      broadcast(r, { type:'join', player:publicPlayer(p), scores:sortedScores(r), teamCounts:teamCounts(r) }, ws);
      broadcast(r, { type:'count', count:r.players.size }); broadcastTeamCounts(r);
      for(const player of r.players.values())send(player.ws,{type:'match-tick',startsAt:r.roundStartsAt,endsAt:r.matchEndsAt});
      return;
    }
    if (!ws.roomCode) return;
    const r = rooms.get(ws.roomCode); const p = r?.players.get(ws.id); if (!r || !p) return;
    if (m.type === 'state') {
      if (r.mode !== 'coop' && p.health <= 0) return;
      const now = Date.now(); if (now - ws.lastUpdate < 50) return; ws.lastUpdate = now;
      p.x = clamp(m.x, -100, 100); p.y = clamp(m.y, -5, 20, 1.7); p.z = clamp(m.z, -100, 100);
      p.yaw = clamp(m.yaw, -20, 20); p.pitch = clamp(m.pitch, -1.6, 1.6);
      p.moving = !!m.moving; p.sprinting = !!m.sprinting; p.aiming = !!m.aiming; p.firing = !!m.firing;p.crouching=!!m.crouching;
      rememberPlayerPosition(p,now);
      broadcast(r, { type: 'state', player: publicPlayer(p) }, ws); return;
    }
    if (m.type === 'respawn-request') {
      if (!['deathmatch','team-deathmatch'].includes(r.mode) || r.matchFinished || p.health > 0) return;
      // Recover safely if an earlier death path failed to leave its cooldown marker.
      // Never trust client position as the respawn result; it is only a candidate.
      if (!Number.isFinite(p.deadAt) || p.deadAt <= 0) {
        p.deadAt = Date.now();
        p.deadX = Number.isFinite(p.x) ? p.x : 0;
        p.deadZ = Number.isFinite(p.z) ? p.z : 0;
      }
      if (Date.now() - p.deadAt < 3900) return;
      const spawn = chooseRespawn(r, p, m);
      if (!spawn) {
        send(ws, { type: 'respawn-rejected' });
        return;
      }
      // Commit the entire new life atomically before broadcasting it.
      p.x = spawn.x; p.y = 1.7; p.z = spawn.z;
      p.health = Number.isFinite(p.maxHealth) && p.maxHealth > 0 ? p.maxHealth : 100;
      p.deadAt = 0;
      delete p.deadX; delete p.deadZ;
      p.moving = false; p.sprinting = false; p.aiming = false; p.firing = false; p.crouching = false;
      p.history = [];
      rememberPlayerPosition(p);
      ws.lastUpdate = 0;
      // Authoritative snapshot includes the new coordinates and full health for all clients.
      const respawned = publicPlayer(p);
      send(ws, { type: 'player-respawn', player: respawned });
      broadcast(r, { type: 'state', player: respawned }, ws);
      broadcast(r, { type: 'player-respawn', player: respawned }, ws);
      return;
    }
    if (m.type === 'player-health') {
      // In competitive play the server owns health; only a real death allows a timed respawn.
      if(r.mode!=='coop') {
        if(p.health>0 || !p.deadAt || Date.now()-p.deadAt<3900 || r.mode==='elimination')return;
        if(clamp(m.health,0,100)!==100)return;
        p.deadAt=0;
      }
      p.health = clamp(m.health, 0, p.maxHealth || 100); if(p.health>0&&r.mode!=='coop')broadcast(r,{type:'player-respawn',player:publicPlayer(p)}); broadcast(r, { type: 'player-health', id: p.id, health: p.health }, ws);
      broadcast(r, { type: 'state', player: publicPlayer(p) }, ws); return;
    }
    if (m.type === 'score-request') { send(ws, { type:'scoreboard', scores:[...(r.scores?.values() || [])] }); return; }
    // Obsolete client-side damage reports are intentionally ignored.
    if (m.type === 'player-hit') return;
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
      const weapon = ['rifle','smg','sniper'].includes(m.weapon) ? m.weapon : 'rifle'; const bodyDamage = weapon === 'smg' ? 5 : weapon === 'sniper' ? 50 : 9; const damage = m.headshot === true ? bodyDamage * 2 : bodyDamage; const wasAlive=!enemy.dead&&enemy.hp>0; enemy.hp = Math.max(0, enemy.hp - damage); if (enemy.hp <= 0) enemy.dead = true;
      if(wasAlive&&enemy.dead){const score=r.scores.get(p.id);if(score)score.kills++;broadcast(r,{type:'killfeed',killerName:p.name||'Opérateur',victimName:'Hostile',weapon:weapon.toUpperCase(),team:p.team||null});broadcast(r,{type:'scoreboard',scores:sortedScores(r)});}
      const result = { type: 'enemy-hit-result', id, hp: enemy.hp, dead: enemy.dead, killer: p.id };
      broadcast(r, result); return;
    }
    if (m.type === 'enemy-player-hit') {
      if (['deathmatch','team-deathmatch','elimination'].includes(r.mode) || r.hostId !== p.id) return;
      const target=r.players.get(String(m.target||''));
      if(!target||target.id===p.id||target.health<=0)return;
      const now=Date.now();if(now-(ws.lastEnemyPlayerHit||0)<650)return;ws.lastEnemyPlayerHit=now;
      const enemy=r.enemies.get(String(m.enemyId||''));if(!enemy||enemy.dead||Math.hypot(target.x-enemy.x,target.z-enemy.z)>4.5)return;
      const damage=clamp(m.damage,1,5,3);target.health=Math.max(0,target.health-damage);
      if(target.health<=0){target.deadAt=Date.now();target.deadX=target.x;target.deadZ=target.z;const score=r.scores.get(target.id);if(score)score.deaths++;send(target.ws,{type:'player-damaged',target:target.id,attacker:'bot',health:0,damage});broadcast(r,{type:'killfeed',killerName:'HOSTILE',victimName:target.name||'Opérateur',weapon:'BOT'});broadcast(r,{type:'scoreboard',scores:sortedScores(r)});}
      else send(target.ws,{type:'player-damaged',target:target.id,attacker:'bot',health:target.health,damage});
      broadcast(r,{type:'state',player:publicPlayer(target)});return;
    }
    if (m.type === 'shot') {
      if(Date.now()<(r.roundStartsAt||0)||r.matchFinished)return;
      const now = Date.now();
      // Normalize shot direction outside the rate-limit branch so tracer broadcast never throws.
      const ox = clamp(m.x, -100, 100), oy = clamp(m.y, -5, 20, 1.7), oz = clamp(m.z, -100, 100);
      let dx = clamp(m.dx, -1, 1), dy = clamp(m.dy, -1, 1), dz = clamp(m.dz, -1, 1);
      const norm = Math.hypot(dx, dy, dz) || 1; dx /= norm; dy /= norm; dz /= norm;
      const weapon = ['rifle','smg','sniper'].includes(m.weapon) ? m.weapon : 'rifle';
      if(Math.hypot(ox-p.x,oy-p.y,oz-p.z)>3.8)return;
      const fireGap = weapon === 'smg' ? 60 : weapon === 'sniper' ? 5000 : 105;
      // m.blocked was computed against detailed client meshes, sometimes including
      // decorative geometry and interpolated player meshes. Never let that local
      // guess suppress a PvP shot: server-side cover is authoritative instead.
      if (now - ws.lastDamageShot >= fireGap) {
        ws.lastDamageShot = now;
        if (['deathmatch','team-deathmatch','elimination'].includes(r.mode)) {
          let best=null,bestT=Infinity,headshot=false;
          const origin={x:ox,y:oy,z:oz},dir={x:dx,y:dy,z:dz};
          const rewindMs=Math.min(215,Math.max(80,65+ws.roundTripMs));
          for (const target of r.players.values()) {
            if (target.id===p.id||target.health<=0||(r.mode==='team-deathmatch'&&target.team===p.team))continue;
            for(const pose of playerShotCandidates(target,now,rewindMs)){
              const impact=playerRayImpact(origin,dir,pose,Math.min(85,bestT));
              if(!impact||impact.t>=bestT)continue;
              // A rewind may help with movement, never with shooting through walls.
              if(coverBefore(r.map,origin,dir,impact.t))continue;
              best=target;bestT=impact.t;headshot=impact.headshot;
            }
          }
          if (best) {
            const bodyDamage = weapon === 'smg' ? 5 : weapon === 'sniper' ? 50 : 9;
            const damage = headshot ? bodyDamage * 2 : bodyDamage;
            best.health = Math.max(0, best.health - damage);
            if (best.health <= 0) {
              best.deadAt=now;best.deadX=best.x;best.deadZ=best.z;
              const killerScore = r.scores.get(p.id), victimScore = r.scores.get(best.id);
              if (killerScore) killerScore.kills++;
              if (victimScore) victimScore.deaths++;
              send(best.ws, { type: 'player-damaged', target: best.id, attacker: p.id, health: 0, damage });
              send(best.ws, { type: 'player-killed', target: best.id, attacker: p.id, attackerName: p.name || 'Opérateur' }); broadcast(r,{type:'killfeed',killerName:p.name||'Opérateur',victimName:best.name||'Opérateur',weapon:weapon.toUpperCase(),team:p.team||null});
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
  // A network socket error must not become an uncaught EventEmitter error.
  ws.on('error', err => console.warn('[ZONE ZERO] Socket error:', err.message));
  ws.on('close', () => {
    if (!ws.roomCode) return; const r = rooms.get(ws.roomCode); if (!r) return;
    r.players.delete(ws.id); r.scores?.delete(ws.id); broadcast(r, { type: 'leave', id: ws.id }); broadcast(r, { type: 'count', count: r.players.size }); broadcast(r, {type:'scoreboard',scores:sortedScores(r)}); broadcastTeamCounts(r);
    if (r.hostId === ws.id) {
      r.hostId = r.players.keys().next().value || null;
      if (r.hostId) { const next = r.players.get(r.hostId); next.isHost = true; send(next.ws, { type: 'host-changed', isHost: true }); }
    }
    if (!r.players.size) { if(r.matchTimer)clearTimeout(r.matchTimer);if(r.matchEndTimer)clearTimeout(r.matchEndTimer);rooms.delete(ws.roomCode); }
  });
});
wss.on('error', err => console.error('[ZONE ZERO] WebSocket server error:', err));
server.on('error', err => {
  console.error('[ZONE ZERO] Impossible de démarrer le serveur :', err);
  process.exitCode = 1;
});
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(ws.isAlive===false){ws.terminate();continue;}ws.isAlive=false;ws.lastWsPingAt=Date.now();ws.ping();}},25000);heartbeat.unref?.();
server.listen(PORT, '0.0.0.0', () => console.log('ZONE ZERO server ready on 0.0.0.0:' + PORT));
