// ===============================
// VLESS Cloudflare Worker Script
// Advanced Features: Multi-UUID, API, Subscription, ProxyIP
// Compatible dengan: v2rayNG, Clash, Surge, Shadowrocket
// ===============================

// ============= KONFIGURASI UTAMA =============
const userIDs = [
  'd342d11e-d424-4583-b36e-524ab1f0afa4',
  'e9e3cc13-db48-4cc1-8c24-7626439a5339',
  'f47ac10b-58cc-4372-a567-0e02b2c3d479'
];

const sub = 'cb.agen.workers.dev'; // Subdomain untuk subscription (opsional)
const subconverter = 'url.v1.mk'; // Subscription converter
const subconfig = "https://raw.githubusercontent.com/ACL4SSR/ACL4SSR/master/Clash/config/ACL4SSR_Online.ini";

// ProxyIP untuk bypass GFW dan ISP blocking
let proxyIPs = [
  'cdn.xn--b6gac.eu.org',
  'cdn-all.xn--b6gac.eu.org', 
  'workers.cloudflare.cyou',
  'www.visa.com',
  'www.visa.com.sg',
  'www.visa.com.hk',
  'www.visa.com.tw',
  'www.visa.co.jp',
  'www.visakorea.com',
  'www.visa.com.au',
  'www.visa.com.br',
  'www.visa.ca',
  'cis.visa.com',
  'africa.visa.com',
  'ap.visa.com',
  'cemea.visa.com',
  'europe.visa.com',
  'lac.visa.com',
  'na.visa.com'
];

let addresses = [
  '146.70.80.1:8080',
  '146.70.80.2:8080', 
  '146.70.80.3:8080',
  '146.70.80.4:8080',
  '146.70.80.5:8080'
];

let addressesapi = [
  'https://raw.githubusercontent.com/vfarid/cf-clean-ips/main/list.txt',
  'https://raw.githubusercontent.com/NiREvil/vless/main/sub/ProxyIP',
  'https://raw.githubusercontent.com/tbbatbb/Proxy/main/dist/cloudflare.txt'
];

let addressesnotls = [
  'cdn.xn--b6gac.eu.org:8080',
  'cdn-all.xn--b6gac.eu.org:8080',
  'workers.cloudflare.cyou:8080'
];

let addressesnotlsapi = [
  'https://raw.githubusercontent.com/vfarid/cf-clean-ips/main/list.txt'
];

let addressescsv = [
  'https://raw.githubusercontent.com/vfarid/cf-clean-ips/main/list.csv'
];

const DLS = 8;
const FileName = 'vless';
const BotToken = '';
const ChatID = ''; 

// ============= MAIN WORKER HANDLER =============
export default {
  async fetch(request, env, ctx) {
    try {
      userIDs.forEach(id => {
        if (!isValidUUID(id)) {
          throw new Error(`Invalid UUID: ${id}`);
        }
      });

      const url = new URL(request.url);
      const token = url.searchParams.get('token');
      
      if (url.pathname.startsWith('/')) {
        if (url.pathname === `/${userIDs[0]}`) {
          const vlessConfig = getVLESSConfig(userIDs[0], request.headers.get('Host'));
          return new Response(`${vlessConfig}`, {
            status: 200,
            headers: {
              "Content-Type": "text/plain;charset=utf-8",
            }
          });
        } else if (url.pathname === `/${userIDs[0]}/ty`) {
          const vlessConfig = getVLESSConfig(userIDs[0], request.headers.get('Host'));
          const now = Date.now();
          const timestamp = Math.floor(now / 1000);
          const today = new Date(now);
          const expire = Math.floor(new Date(today.getFullYear(), today.getMonth() + 1, today.getDate()).getTime() / 1000);
          const total = 99 * 1024 * 1024 * 1024;
          return new Response(`${vlessConfig}`, {
            status: 200,
            headers: {
              "Content-Type": "text/plain;charset=utf-8",
              "subscription-userinfo": `upload=0; download=0; total=${total}; expire=${expire}`,
            }
          });
        }
      }

      if (url.pathname.startsWith('/sub')) {
        const host = request.headers.get('Host');
        const userAgent = request.headers.get('User-Agent') || 'v2rayN/6.23';
        const subscriptionContent = await generateSubscription(userIDs, host, userAgent, url);
        
        return new Response(btoa(subscriptionContent), {
          headers: {
            'content-type': 'text/plain; charset=utf-8',
            'cache-control': 'no-cache'
          }
        });
      }

      if (url.pathname.startsWith('/bestip')) {
        const headers = request.headers;
        const html = getBestIPHTML();
        return new Response(html, {
          headers: {
            'content-type': 'text/html; charset=utf-8',
          },
        });
      }

      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        const url = new URL(request.url);
        switch (url.pathname) {
          case '/':
            return new Response(getIndexHTML(request), {
              headers: {
                'content-type': 'text/html; charset=utf-8',
              },
            });
          case `/${userIDs[0]}`:
            return new Response(getVLESSConfig(userIDs[0], request.headers.get('Host')), {
              headers: {
                'content-type': 'text/html; charset=utf-8',
              },
            });
          default:
            return new Response('Not found', { status: 404 });
        }
      } else {
        return await vlessOverWSHandler(request);
      }
    } catch (err) {
      return new Response(err.toString());
    }
  },
};

// ============= VLESS WEBSOCKET HANDLER =============
async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  let address = '';
  let portWithRandomLog = '';
  const log = (info, event) => {
    console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
  };
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

  let remoteSocketWapper = {
    value: null,
  };
  let udpStreamWrite = null;
  let isDns = false;

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (isDns && udpStreamWrite) {
        return await udpStreamWrite(chunk);
      }
      if (remoteSocketWapper.value) {
        const writer = remoteSocketWapper.value.writable.getWriter()
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const {
        hasError,
        message,
        portRemote = 443,
        addressRemote = '',
        rawDataIndex,
        vlessVersion = new Uint8Array([0, 0]),
        isUDP,
      } = processVlessHeader(chunk, userIDs);
      address = addressRemote;
      portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '} `;
      if (hasError) {
        throw new Error(message);
        return;
      }
      if (isUDP) {
        if (portRemote === 53) {
          isDns = true;
        } else {
          throw new Error('UDP proxy only enable for DNS which is port 53');
          return;
        }
      }
      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isDns) {
        const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
        udpStreamWrite = write;
        udpStreamWrite(rawClientData);
        return;
      }
      handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
    },
    close() {
      log(`readableWebSocketStream is close`);
    },
    abort(reason) {
      log(`readableWebSocketStream is abort`, JSON.stringify(reason));
    },
  })).catch((err) => {
    log('readableWebSocketStream pipeTo error', err);
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

// ============= VLESS PROTOCOL PROCESSING =============
function processVlessHeader(vlessBuffer, userIDs) {
  if (vlessBuffer.byteLength < 24) {
    return {
      hasError: true,
      message: 'invalid data',
    };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = false;
  let isUDP = false;
  const slicedBuffer = new Uint8Array(vlessBuffer.slice(1, 17));
  const byteArray = Array.from(slicedBuffer);
  const uuidString = stringify(byteArray);

  const checkUuid = userIDs.some(userID => checkUuidEndsWith(uuidString, userID));
  console.log(`checkUuid: ${checkUuid}`);

  isValidUser = checkUuid;
  if (!isValidUser) {
    return {
      hasError: true,
      message: 'invalid user',
    };
  }

  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];

  if (command === 1) {
    isUDP = false;
  } else if (command === 2) {
    isUDP = true;
  } else {
    return {
      hasError: true,
      message: `command ${command} is not support, command 01-tcp,02-udp,03-mux`,
    };
  }
  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);

  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));

  const addressType = addressBuffer[0];
  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';
  switch (addressType) {
    case 1:
      addressLength = 4;
      addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
      break;
    case 2:
      addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3:
      addressLength = 16;
      const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      break;
    default:
      return {
        hasError: true,
        message: `invild  addressType is ${addressType}`,
      };
  }
  if (!addressValue) {
    return {
      hasError: true,
      message: `addressValue is empty, addressType is ${addressType}`,
    };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

// ============= TCP OUTBOUND HANDLER =============
async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log) {
  async function connectAndWrite(address, port) {
    if (/^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$/.test(address)) address = `${atob('d3d3Lg==')}${address}${atob('LnNzbGlwLmlv')}`;
    const tcpSocket = connect({
      hostname: address,
      port: port,
    });
    remoteSocket.value = tcpSocket;
    log(`connected to ${address}:${port}`);
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  async function retry() {
    if (proxyIPs.length === 0) {
      console.log('Tidak ada proxyIP yang tersisa untuk dicoba');
      return;
    }
    const randomProxyIP = proxyIPs[Math.floor(Math.random() * proxyIPs.length)];
    const tcpSocket = await connectAndWrite(randomProxyIP, portRemote);
    tcpSocket.closed.catch(error => {
      console.log('Koneksi ke', randomProxyIP, 'tertutup:', error);
    }).finally(() => {
      proxyIPs = proxyIPs.filter(ip => ip !== randomProxyIP);
    });
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
  }

  const tcpSocket = await connectAndWrite(addressRemote, portRemote);
  remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
}

// ============= WEBSOCKET UTILITIES =============
function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
  let readableStreamCancel = false;
  const stream = new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) {
          return;
        }
        const message = event.data;
        controller.enqueue(message);
      });

      webSocketServer.addEventListener('close', () => {
        if (readableStreamCancel) {
          return;
        }
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        log('webSocketServer has error');
        controller.error(err);
      });
      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },

    pull(controller) {
    },

    cancel(reason) {
      if (readableStreamCancel) {
        return;
      }
      log(`ReadableStream was canceled, due to ${reason}`);
      readableStreamCancel = true;
      webSocketServer.close();
    }
  });

  return stream;
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
  let remoteChunkCount = 0;
  let chunks = [];
  let vlessHeader = vlessResponseHeader;
  let hasIncomingData = false;

  await remoteSocket.readable
    .pipeTo(
      new WritableStream({
        start() {
        },
        async write(chunk, controller) {
          hasIncomingData = true;
          if (webSocket.readyState !== WS_READY_STATE_OPEN) {
            controller.error('webSocket.readyState is not open, maybe close');
          }
          if (vlessHeader) {
            webSocket.send(await new Blob([vlessHeader, chunk]).arrayBuffer());
            vlessHeader = null;
          } else {
            webSocket.send(chunk);
          }
        },
        close() {
          log(`remoteConnection!.readable is close with hasIncomingData is ${hasIncomingData}`);
        },
        abort(reason) {
          console.error(`remoteConnection!.readable abort`, reason);
        },
      })
    )
    .catch((error) => {
      console.error(`remoteSocketToWS has exception `, error.stack || error);
      safeCloseWebSocket(webSocket);
    });

  if (hasIncomingData === false && retry) {
    log(`retry`);
    retry();
  }
}

// ============= DNS OVER UDP HANDLER =============
async function handleUDPOutBound(webSocket, vlessResponseHeader, log) {
  let isVlessHeaderSent = false;
  const transformStream = new TransformStream({
    start(controller) {
    },
    transform(chunk, controller) {
      for (let index = 0; index < chunk.byteLength;) {
        const lengthBuffer = chunk.slice(index, index + 2);
        const udpPakcetLength = new DataView(lengthBuffer).getUint16(0);
        const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPakcetLength));
        index = index + 2 + udpPakcetLength;
        controller.enqueue(udpData);
      }
    },
    flush(controller) {
    }
  });

  transformStream.readable
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          const resp = await fetch('https://1.1.1.1/dns-query',
            {
              method: 'POST',
              headers: {
                'content-type': 'application/dns-message',
              },
              body: chunk,
            })
          const dnsQueryResult = await resp.arrayBuffer();
          const udpSize = dnsQueryResult.byteLength;
          const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
          if (webSocket.readyState === WS_READY_STATE_OPEN) {
            log(`doh success and dns message length is ${udpSize}`);
            if (isVlessHeaderSent) {
              webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
            } else {
              webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
              isVlessHeaderSent = true;
            }
          }
        }
      })
    )
    .catch((error) => {
      log('dns udp has error' + error)
    });

  const writer = transformStream.writable.getWriter();

  return {
    write(chunk) {
      writer.write(chunk);
    }
  };
}

// ============= SUBSCRIPTION GENERATOR =============
async function generateSubscription(userIDs, hostName, userAgent, url) {
  const subParams = url.searchParams;
  const vlessConfigs = [];

  for (const userID of userIDs) {
    const commonUrlPart = `:443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#`;

    // Add different types of configs
    vlessConfigs.push(`vless://${userID}@${hostName}${commonUrlPart}${encodeURIComponent(`🚩CF-${hostName}`)}`);
    
    // Add proxyIP configs
    for (let i = 0; i < Math.min(proxyIPs.length, 5); i++) {
      vlessConfigs.push(`vless://${userID}@${proxyIPs[i]}${commonUrlPart}${encodeURIComponent(`🚩CF-${proxyIPs[i]}`)}`);
    }

    // Add different ports for variety
    const ports = [80, 8080, 8443, 2052, 2082, 2086, 2095];
    for (const port of ports.slice(0, 3)) {
      if (port !== 443) {
        const portUrlPart = `:${port}?encryption=none&security=${port === 80 ? 'none' : 'tls'}&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#`;
        vlessConfigs.push(`vless://${userID}@${hostName}${portUrlPart}${encodeURIComponent(`🚩CF-${hostName}-${port}`)}`);
      }
    }
  }

  return vlessConfigs.join('\n');
}

// ============= VLESS CONFIG GENERATOR =============
function getVLESSConfig(userID, hostName) {
  const vlessMain = `vless://${userID}@${hostName}:443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#${encodeURIComponent(`🚩CF-${hostName}`)}`;
  return `${vlessMain}`;
}

// ============= HTML RESPONSES =============
function getIndexHTML(request) {
  const url = new URL(request.url);
  const host = url.hostname;
  
  return `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 VLESS Cloudflare Worker</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333; min-height: 100vh; padding: 20px;
        }
        .container { max-width: 900px; margin: 0 auto; }
        .header { text-align: center; margin-bottom: 40px; }
        .header h1 { color: white; font-size: 3rem; margin-bottom: 10px; text-shadow: 0 2px 4px rgba(0,0,0,0.3); }
        .status { background: rgba(255,255,255,0.95); padding: 20px; border-radius: 15px; margin-bottom: 25px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .status h2 { color: #4CAF50; margin-bottom: 15px; display: flex; align-items: center; }
        .status h2:before { content: "✅"; margin-right: 10px; font-size: 1.2em; }
        .config-section { background: rgba(255,255,255,0.95); padding: 25px; border-radius: 15px; margin-bottom: 25px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .config-section h3 { color: #2196F3; margin-bottom: 20px; font-size: 1.3em; }
        .config-item { background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 15px; border-left: 4px solid #2196F3; }
        .config-item label { font-weight: bold; color: #333; display: block; margin-bottom: 5px; }
        .config-item .value { font-family: monospace; background: #e9ecef; padding: 8px; border-radius: 4px; word-break: break-all; font-size: 0.9em; }
        .btn { display: inline-block; background: #2196F3; color: white; padding: 12px 24px; text-decoration: none; border-radius: 8px; margin: 5px; transition: all 0.3s; border: none; cursor: pointer; font-size: 1em; }
        .btn:hover { background: #1976D2; transform: translateY(-2px); }
        .btn-success { background: #4CAF50; }
        .btn-success:hover { background: #45a049; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .copy-btn { background: #FF9800; font-size: 0.9em; padding: 8px 16px; margin-left: 10px; }
        .copy-btn:hover { background: #F57C00; }
        .note { background: #fff3cd; border: 1px solid #ffeaa7; color: #856404; padding: 15px; border-radius: 8px; margin: 15px 0; }
        .uuid-list { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }
        .uuid-item { background: #e3f2fd; color: #1565c0; padding: 8px 12px; border-radius: 6px; font-family: monospace; font-size: 0.85em; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 VLESS Worker</h1>
            <p style="color: rgba(255,255,255,0.9); font-size: 1.2em;">High Performance Proxy Service</p>
        </div>

        <div class="status">
            <h2>服务状态</h2>
            <p><strong>域名:</strong> ${host}</p>
            <p><strong>协议:</strong> VLESS over WebSocket + TLS</p>
            <p><strong>节点数量:</strong> ${proxyIPs.length} 个全球节点</p>
            <div class="uuid-list">
                ${userIDs.map(id => `<span class="uuid-item">${id}</span>`).join('')}
            </div>
        </div>

        <div class="grid">
            <div class="config-section">
                <h3>📱 v2rayNG 配置</h3>
                <div class="config-item">
                    <label>服务器地址:</label>
                    <div class="value">${host}</div>
                </div>
                <div class="config-item">
                    <label>端口:</label>
                    <div class="value">443</div>
                </div>
                <div class="config-item">
                    <label>用户ID:</label>
                    <div class="value">${userIDs[0]}</div>
                </div>
                <div class="config-item">
                    <label>传输协议:</label>
                    <div class="value">ws (WebSocket)</div>
                </div>
                <div class="config-item">
                    <label>路径:</label>
                    <div class="value">/?ed=2048</div>
                </div>
                <div class="config-item">
                    <label>TLS:</label>
                    <div class="value">开启</div>
                </div>
            </div>

            <div class="config-section">
                <h3>🔗 快速导入</h3>
                <div class="config-item">
                    <label>VLESS 链接:</label>
                    <div class="value" id="vlessLink">${getVLESSConfig(userIDs[0], host)}</div>
                    <button class="btn copy-btn" onclick="copyToClipboard('vlessLink')">复制链接</button>
                </div>
                <div class="config-item">
                    <label>订阅链接:</label>
                    <div class="value" id="subLink">https://${host}/sub</div>
                    <button class="btn copy-btn" onclick="copyToClipboard('subLink')">复制订阅</button>
                </div>
            </div>
        </div>

        <div class="config-section">
            <h3>🌐 全球节点</h3>
            <p>本服务提供 ${proxyIPs.length} 个全球高速节点，自动选择最优线路：</p>
            <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; margin-top: 15px;">
                ${proxyIPs.slice(0, 12).map(