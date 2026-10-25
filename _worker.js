import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env, ctx) {
    const userID = env.UUID || "89b3cbba-0a45-4f65-8a01-1b3b8a8c8b1a";
    const url = new URL(request.url);
    const host = request.headers.get("Host");

    if (url.pathname === "/") {
      const vlessURL = `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=randomized&type=ws&host=${host}&path=%2F%3Fed%3D2048#${host}`;
      return new Response(`Ваша ссылка для Karing:\n\n${vlessURL}`, {
        headers: { "Content-Type": "text/plain;charset=utf-8" }
      });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    handleSession(server, userID);

    return new Response(null, { status: 101, webSocket: client });
  }
};

async function handleSession(ws, userID) {
  let remoteSocket;
  
  ws.addEventListener('message', async (event) => {
    if (remoteSocket) {
        const writer = remoteSocket.writable.getWriter();
        await writer.write(new Uint8Array(event.data));
        writer.releaseLock();
        return;
    }
    
    let buffer = new Uint8Array(event.data);
    if (buffer.byteLength < 24) return;
    
    const uuidBytes = buffer.slice(1, 17);
    const uuidHex = Array.from(uuidBytes).map(b => b.toString(16).padStart(2, '0')).join('');
    const parsedUUID = `${uuidHex.slice(0,8)}-${uuidHex.slice(8,12)}-${uuidHex.slice(12,16)}-${uuidHex.slice(16,20)}-${uuidHex.slice(20)}`;
    
    if (parsedUUID !== userID) {
        ws.close();
        return;
    }
    
    let optLength = buffer[17];
    let cmd = buffer[18 + optLength];
    let portIndex = 18 + optLength + 1;
    
    let addrType = buffer[portIndex];
    let port = new DataView(buffer.buffer, portIndex + 1, 2).getUint16(0);
    let address = "";
    let addrIndex = portIndex + 3;
    
    if (addrType === 1) {
        address = new Uint8Array(buffer.slice(addrIndex, addrIndex + 4)).join('.');
        addrIndex += 4;
    } else if (addrType === 2) {
        let len = buffer[addrIndex];
        address = new TextDecoder().decode(buffer.slice(addrIndex + 1, addrIndex + 1 + len));
        addrIndex += 1 + len;
    } else if (addrType === 3) {
        let ipv6 = [];
        for (let i = 0; i < 8; i++) {
            ipv6.push(new DataView(buffer.buffer, addrIndex + i * 2, 2).getUint16(0).toString(16));
        }
        address = ipv6.join(':');
        addrIndex += 16;
    }
    
    let payload = buffer.slice(addrIndex);
    
    ws.send(new Uint8Array([buffer[0], 0])); 
    
    if (cmd === 1) {
        try {
            remoteSocket = connect({ hostname: address, port: port });
            const writer = remoteSocket.writable.getWriter();
            if (payload.byteLength > 0) await writer.write(payload);
            writer.releaseLock();
            
            remoteSocket.readable.pipeTo(new WritableStream({
                async write(chunk) {
                    ws.send(chunk);
                },
                close() { ws.close(); },
                abort() { ws.close(); }
            }));
            
            ws.addEventListener('message', async (ev) => {
                let chunk = new Uint8Array(ev.data);
                if (chunk.byteLength > 0) {
                    const w = remoteSocket.writable.getWriter();
                    await w.write(chunk);
                    w.releaseLock();
                }
            });
            
            ws.addEventListener('close', async () => {
                remoteSocket.close();
            });
        } catch (e) {
            ws.close();
        }
    }
  });
}