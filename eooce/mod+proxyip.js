import { connect } from 'cloudflare:sockets';

// ==================================================================================
// 用户配置区域 (User Configuration)
// ==================================================================================

// 您的 VLESS UUID，用于身份验证。
let yourUUID = '93bf61d9-3796-44c2-9b3a-49210ece2585';

// [核心功能] 默认的全局备用 ProxyIP (Fallback ProxyIP)
// 当直连目标地址失败时，会使用这个地址作为备用。
// 如果区域匹配功能开启，此设置将被区域匹配结果覆盖。
let proxyIP = '13.230.34.30';

// Cloudflare CDN 优选域名/IP列表，用于生成订阅链接。
// 在此感谢各位大佬维护的优选域名。
let cfip = [
    'mfa.gov.ua', 'saas.sin.fan', 'store.ubi.com','cf.130519.xyz','cf.008500.xyz',
    'cf.090227.xyz', 'cf.877774.xyz','cdns.doon.eu.org','sub.danfeng.eu.org','cf.zhetengsha.eu.org'
];

// ==================================================================================
// 区域匹配 ProxyIP 功能配置 (Regional ProxyIP Matching Feature)
// ==================================================================================

// 是否启用区域匹配功能。true: 启用, false: 禁用 (将使用上面的全局 proxyIP)。
let enableRegionMatching = true;

// 区域化 ProxyIP 列表。
// 当区域匹配功能启用时，程序会根据 Worker 节点的地理位置，优先选择相同区域的 ProxyIP。
// domain: ProxyIP 域名或 IP
// regionCode: 区域代码 (两位大写字母)
// port: 端口号
const backupIPs = [
    { domain: 'ProxyIP.US.CMLiussss.net', regionCode: 'US', port: 443 },
    { domain: 'ProxyIP.SG.CMLiussss.net', regionCode: 'SG', port: 443 },
    { domain: 'ProxyIP.JP.CMLiussss.net', regionCode: 'JP', port: 443 },
    { domain: 'ProxyIP.HK.CMLiussss.net', regionCode: 'HK', port: 443 },
    { domain: 'ProxyIP.KR.CMLiussss.net', regionCode: 'KR', port: 443 },
    { domain: 'ProxyIP.DE.CMLiussss.net', regionCode: 'DE', port: 443 },
    { domain: 'ProxyIP.SE.CMLiussss.net', regionCode: 'SE', port: 443 },
    { domain: 'ProxyIP.NL.CMLiussss.net', regionCode: 'NL', port: 443 },
    { domain: 'ProxyIP.FI.CMLiussss.net', regionCode: 'FI', port: 443 },
    { domain: 'ProxyIP.GB.CMLiussss.net', regionCode: 'GB', port: 443 },
    { domain: 'ProxyIP.Oracle.cmliussss.net', regionCode: 'Oracle', port: 443 }, // 可自定义 Tag，例如 ISP
    { domain: 'ProxyIP.DigitalOcean.CMLiussss.net', regionCode: 'DigitalOcean', port: 443 },
    { domain: 'ProxyIP.Vultr.CMLiussss.net', regionCode: 'Vultr', port: 443 },
    { domain: 'ProxyIP.Multacom.CMLiussss.net', regionCode: 'Multacom', port: 443 }
];


// ==================================================================================
// 主程序逻辑 (Main Logic) - 一般无需修改
// ==================================================================================

export default {
	async fetch(request, env, ctx) {
		try {
			const url = new URL(request.url);
			// 优先处理 WebSocket 升级请求
			if (request.headers.get('Upgrade') === 'websocket') {
				const customProxyIP = url.searchParams.get('proxyip');
				return await handleWsRequest(request, customProxyIP);
			}

			// 处理 GET 请求
			if (request.method === 'GET') {
				if (url.pathname === '/') {
					return handleHomePage(request);
				}
				if (url.pathname === `/${yourUUID}`) {
					return handleSubPage(request);
				}
				if (url.pathname.toLowerCase().startsWith(`/sub/${yourUUID.toLowerCase()}`)) {
					const currentDomain = url.hostname;
					const vlessProtocol = 'v' + 'l' + 'e' + 's' + 's'; // 避免被检测
					const nodeLinks = cfip.map(cdn => {
						return `${vlessProtocol}://${yourUUID}@${cdn}:443?encryption=none&security=tls&sni=${currentDomain}&fp=firefox&allowInsecure=1&type=ws&host=${currentDomain}&path=%2F%3Fed%3D2560#Snippets-${vlessProtocol}`;
					});
					const linksText = nodeLinks.join('\n');
					const base64Content = btoa(linksText);

					return new Response(base64Content, {
						headers: {
							'Content-Type': 'text/plain; charset=utf-8',
							'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
						},
					});
				}
			}

			// 其他所有请求返回 404
			return new Response('Not Found', { status: 404 });
		} catch (err) {
			return new Response(err.toString(), { status: 500 });
		}
	},
};

/**
 * 处理 WebSocket 请求
 * @param {Request} request 传入的请求
 * @param {string|null} customProxyIP 从 URL 参数传入的自定义 ProxyIP
 */
async function handleWsRequest(request, customProxyIP) {
	const wsPair = new WebSocketPair();
	const [clientSock, serverSock] = Object.values(wsPair);
	serverSock.accept();

	let remoteConnWrapper = { socket: null };
	let isDnsQuery = false;

	// 从请求头获取早期数据
	const earlyData = request.headers.get('sec-websocket-protocol') || '';
	const readable = makeReadableStream(serverSock, earlyData);

	// [新增] 在处理 WebSocket 请求时检测区域
	const detectedRegion = await detectWorkerRegion(request);

	readable.pipeTo(new WritableStream({
		async write(chunk) {
			if (isDnsQuery) return await forwardUDP(chunk, serverSock, null);
			if (remoteConnWrapper.socket) {
				const writer = remoteConnWrapper.socket.writable.getWriter();
				await writer.write(chunk);
				writer.releaseLock();
				return;
			}
			const { hasError, message, addressType, port, hostname, rawIndex, version, isUDP } = parseWsPacketHeader(chunk, yourUUID);
			if (hasError) throw new Error(message);

			if (isUDP) {
				if (port === 53) isDnsQuery = true;
				else throw new Error('UDP proxy is only enabled for DNS queries');
			}
			const respHeader = new Uint8Array([version[0], 0]);
			const rawData = chunk.slice(rawIndex);

			if (isDnsQuery) return forwardUDP(rawData, serverSock, respHeader);

			// [修改] 将检测到的区域传递给 TCP 转发函数
			await forwardTCP(hostname, port, rawData, serverSock, respHeader, remoteConnWrapper, customProxyIP, detectedRegion);
		},
		abort(reason) {
			// console.log('Readable side aborted:', reason);
		},
	})).catch((err) => {
		// console.error('Readable pipe error:', err);
		closeSocketQuietly(serverSock);
	});

	return new Response(null, { status: 101, webSocket: clientSock });
}

/**
 * 核心函数：转发 TCP 流量
 * @param {string} host 目标主机名
 * @param {number} portNum 目标端口
 * @param {Uint8Array} rawData 初始数据
 * @param {WebSocket} ws 客户端 WebSocket
 * @param {Uint8Array} respHeader VLESS 响应头
 * @param {object} remoteConnWrapper 远程连接的包装对象
 * @param {string|null} customProxyIP 自定义 ProxyIP
 * @param {string} detectedRegion 检测到的 Worker 区域代码
 */
async function forwardTCP(host, portNum, rawData, ws, respHeader, remoteConnWrapper, customProxyIP, detectedRegion) {

	// 尝试直连的函数
	async function connectDirect(address, port, data) {
		const remoteSock = connect({ hostname: address, port: port });
		const writer = remoteSock.writable.getWriter();
		await writer.write(data);
		writer.releaseLock();
		return remoteSock;
	}

	// 备用连接函数 (Proxy
	async function connectWithProxy() {
		let fallbackHost = proxyIP;
		let fallbackPort = 443;
		let proxySource = "Global Default";

		// 如果提供了自定义 proxyip 参数，则优先使用它
		if (customProxyIP) {
			const parsedCustom = parseProxyAddress(customProxyIP);
			if (parsedCustom) {
				fallbackHost = parsedCustom.host;
				fallbackPort = parsedCustom.port;
				proxySource = "URL Parameter";
			}
		} else if (enableRegionMatching) {
			// [核心修改]
			// 如果启用了区域匹配，则获取最佳区域 ProxyIP
			const bestBackup = await getBestBackupIP(detectedRegion);
			if (bestBackup) {
				fallbackHost = bestBackup.domain;
				fallbackPort = bestBackup.port;
				proxySource = `Regional Match (${detectedRegion} -> ${bestBackup.regionCode})`;
			} else {
				proxySource = "Regional Match (Not Found)";
			}
		}

		// console.log(`Fallback via: ${fallbackHost}:${fallbackPort} (Source: ${proxySource})`);

		try {
			const fallbackSocket = await connectDirect(fallbackHost, fallbackPort, rawData);
			remoteConnWrapper.socket = fallbackSocket;
			fallbackSocket.closed.catch(() => {}).finally(() => closeSocketQuietly(ws));
			connectStreams(fallbackSocket, ws, respHeader, null); // 备用连接不再重试
		} catch (fallbackErr) {
			// console.error('Fallback connection failed:', fallbackErr);
			closeSocketQuietly(ws);
		}
	}

	// 主逻辑：首先尝试直连
	try {
		const initialSocket = await connectDirect(host, portNum, rawData);
		remoteConnWrapper.socket = initialSocket;
		connectStreams(initialSocket, ws, respHeader, connectWithProxy);
	} catch (err) {
		// 直连失败，调用备用连接函数
		// console.log(`Direct connection to ${host}:${portNum} failed, trying fallback...`);
		await connectWithProxy();
	}
}


// ==================================================================================
// 区域匹配功能函数 (Functions for Regional Matching)
// ==================================================================================

/**
 * 检测 Worker 所在区域
 * @param {Request} request
 * @returns {Promise<string>} 区域代码 (例如 'HK')
 */
async function detectWorkerRegion(request) {
	try {
		const cfCountry = request.cf?.country;
		if (cfCountry) {
			// 简单的国家到区域的映射表，可以根据需要扩展
			const countryToRegion = {
				'US': 'US', 'SG': 'SG', 'JP': 'JP', 'HK': 'HK', 'KR': 'KR',
				'DE': 'DE', 'SE': 'SE', 'NL': 'NL', 'FI': 'FI', 'GB': 'GB',
				// 将亚洲常见国家映射到最近的节点
				'CN': 'HK', 'TW': 'HK', 'AU': 'SG',
				// 将北美映射到美国
				'CA': 'US',
				// 将欧洲常见国家映射到最近的节点
				'FR': 'DE', 'IT': 'DE', 'ES': 'DE', 'CH': 'DE',
				'AT': 'DE', 'BE': 'NL', 'DK': 'SE', 'NO': 'SE', 'IE': 'GB'
			};
			if (countryToRegion[cfCountry]) {
				return countryToRegion[cfCountry];
			}
		}
		// 默认返回香港
		return 'HK';
	} catch (error) {
		// 发生错误时默认返回香港
		return 'HK';
	}
}

/**
 * 根据 Worker 区域获取最佳的备用 ProxyIP
 * @param {string} workerRegion Worker 的区域代码
 * @returns {Promise<object|null>} 最佳的 ProxyIP 对象或 null
 */
async function getBestBackupIP(workerRegion = '') {
	if (backupIPs.length === 0) {
		return null;
	}
	if (enableRegionMatching && workerRegion) {
		const sortedIPs = getSmartRegionSelection(workerRegion, backupIPs);
		return sortedIPs.length > 0 ? sortedIPs[0] : backupIPs[0];
	}
	// 如果不启用区域匹配或没有区域信息，返回第一个
	return backupIPs[0];
}

/**
 * 获取一个区域的邻近区域列表
 * @param {string} region
 * @returns {string[]}
 */
function getNearbyRegions(region) {
	const nearbyMap = {
		'US': ['CA', 'GB', 'DE'],
		'SG': ['HK', 'JP', 'KR', 'AU'],
		'JP': ['KR', 'HK', 'SG'],
		'HK': ['SG', 'JP', 'KR', 'TW'],
		'KR': ['JP', 'HK', 'SG'],
		'DE': ['NL', 'GB', 'FR', 'SE', 'FI'],
		'SE': ['FI', 'DE', 'NL', 'GB'],
		'NL': ['DE', 'GB', 'BE', 'FR'],
		'FI': ['SE', 'DE'],
		'GB': ['IE', 'NL', 'DE']
	};
	return nearbyMap[region] || [];
}

/**
 * 获取所有区域的优先级列表
 * @param {string} region
 * @returns {string[]}
 */
function getAllRegionsByPriority(region) {
	const nearbyRegions = getNearbyRegions(region);
	const allRegions = [...new Set(backupIPs.map(ip => ip.regionCode))];
	// 优先级: 本区域 -> 邻近区域 -> 其他所有区域
	return [
		region,
		...nearbyRegions,
		...allRegions.filter(r => r !== region && !nearbyRegions.includes(r))
	];
}

/**
 * 智能排序可用的 ProxyIP 列表
 * @param {string} workerRegion
 * @param {Array<object>} availableIPs
 * @returns {Array<object>}
 */
function getSmartRegionSelection(workerRegion, availableIPs) {
	if (!enableRegionMatching || !workerRegion) {
		return availableIPs;
	}
	const priorityRegions = getAllRegionsByPriority(workerRegion);
	const sortedIPs = [];
	for (const region of priorityRegions) {
		const regionIPs = availableIPs.filter(ip => ip.regionCode === region);
		sortedIPs.push(...regionIPs);
	}
	return sortedIPs;
}

// ==================================================================================
// 辅助函数 (Helper Functions)
// ==================================================================================

/**
 * 解析 VLESS over WS 的数据包头部
 */
function parseWsPacketHeader(chunk, token) {
	if (chunk.byteLength < 24) return { hasError: true, message: 'Invalid data: chunk too short' };
	const version = new Uint8Array(chunk.slice(0, 1));
	if (formatIdentifier(new Uint8Array(chunk.slice(1, 17))) !== token) return { hasError: true, message: 'Invalid UUID' };

	const optLen = new Uint8Array(chunk.slice(17, 18))[0];
	const cmd = new Uint8Array(chunk.slice(18 + optLen, 19 + optLen))[0];
	let isUDP = (cmd === 2);
	if (cmd !== 1 && cmd !== 2) return { hasError: true, message: 'Unsupported command' };

	const portIdx = 19 + optLen;
	const port = new DataView(chunk.slice(portIdx, portIdx + 2)).getUint16(0);

	let addrIdx = portIdx + 2;
	const addressType = new Uint8Array(chunk.slice(addrIdx, addrIdx + 1))[0];
	addrIdx += 1;

	let hostname = '';
	let addrLen = 0;
	switch (addressType) {
		case 1: // IPv4
			addrLen = 4;
			hostname = new Uint8Array(chunk.slice(addrIdx, addrIdx + addrLen)).join('.');
			break;
		case 2: // Domain
			addrLen = new Uint8Array(chunk.slice(addrIdx, addrIdx + 1))[0];
			addrIdx += 1;
			hostname = new TextDecoder().decode(chunk.slice(addrIdx, addrIdx + addrLen));
			break;
		case 3: // IPv6
			addrLen = 16;
			const ipv6 = [];
			const ipv6View = new DataView(chunk.slice(addrIdx, addrIdx + addrLen));
			for (let i = 0; i < 8; i++) ipv6.push(ipv6View.getUint16(i * 2).toString(16));
			hostname = ipv6.join(':');
			break;
		default:
			return { hasError: true, message: `Invalid address type: ${addressType}` };
	}

	if (!hostname) return { hasError: true, message: 'Hostname is empty' };

	return { hasError: false, addressType, port, hostname, isUDP, rawIndex: addrIdx + addrLen, version };
}

/**
 * 将 WebSocket 转换为可读流
 */
function makeReadableStream(socket, earlyDataHeader) {
	let cancelled = false;
	return new ReadableStream({
		start(controller) {
			socket.addEventListener('message', (event) => {
				if (!cancelled) controller.enqueue(event.data);
			});
			socket.addEventListener('close', () => {
				if (!cancelled) {
					closeSocketQuietly(socket);
					controller.close();
				}
			});
			socket.addEventListener('error', (err) => controller.error(err));
			const { earlyData, error } = base64ToArray(earlyDataHeader);
			if (error) controller.error(error);
			else if (earlyData) controller.enqueue(earlyData);
		},
		cancel() {
			cancelled = true;
			closeSocketQuietly(socket);
		}
	});
}

/**
 * 连接远程 Socket 和 WebSocket 的流
 */
async function connectStreams(remoteSocket, webSocket, headerData, retryFunc) {
	let header = headerData;
	let hasData = false;
	await remoteSocket.readable.pipeTo(
		new WritableStream({
			async write(chunk) {
				hasData = true;
				if (webSocket.readyState !== WebSocket.OPEN) {
					throw new Error('WebSocket is not open');
				}
				if (header) {
					const response = new Uint8Array(header.length + chunk.byteLength);
					response.set(header, 0);
					response.set(chunk, header.length);
					webSocket.send(response.buffer);
					header = null;
				} else {
					webSocket.send(chunk);
				}
			},
			abort(reason) {
				// console.error(`Remote readable aborted: ${reason}`);
			},
		})
	).catch((error) => {
		// console.error(`Failed to pipe remoteSocket to webSocket: ${error}`);
		closeSocketQuietly(webSocket);
	});

	if (!hasData && retryFunc) {
		await retryFunc();
	}
}

/**
 * 转发 UDP 流量 (DNS 查询)
 */
async function forwardUDP(udpChunk, webSocket, respHeader) {
	try {
        // 使用 Cloudflare 的 DNS 服务
		const tcpSocket = connect({ hostname: '1.1.1.1', port: 53 });
		let vlessHeader = respHeader;
		const writer = tcpSocket.writable.getWriter();
		await writer.write(udpChunk);
		writer.releaseLock();

		await tcpSocket.readable.pipeTo(new WritableStream({
			async write(chunk) {
				if (webSocket.readyState === WebSocket.OPEN) {
					if (vlessHeader) {
						const response = new Uint8Array(vlessHeader.length + chunk.byteLength);
						response.set(vlessHeader, 0);
						response.set(chunk, vlessHeader.length);
						webSocket.send(response.buffer);
						vlessHeader = null;
					} else {
						webSocket.send(chunk);
					}
				}
			},
		}));
	} catch (error) {
		// console.error('UDP forward error:', error);
	}
}

/**
 * 解析各种格式的 Proxy 地址字符串
 */
function parseProxyAddress(proxyStr) {
	if (!proxyStr) return null;
	proxyStr = proxyStr.trim();
	// IPv6 地址: [host]:port
	if (proxyStr.startsWith('[')) {
		const closeBracket = proxyStr.indexOf(']');
		if (closeBracket > 0) {
			const host = proxyStr.substring(1, closeBracket);
			const rest = proxyStr.substring(closeBracket + 1);
			if (rest.startsWith(':')) {
				const port = parseInt(rest.substring(1), 10);
				if (!isNaN(port) && port > 0 && port <= 65535) {
					return { host, port };
				}
			}
			return { host, port: 443 };
		}
	}
	// IPv4 或域名: host:port
	const lastColonIndex = proxyStr.lastIndexOf(':');
	if (lastColonIndex > 0) {
		const host = proxyStr.substring(0, lastColonIndex);
		const portStr = proxyStr.substring(lastColonIndex + 1);
		const port = parseInt(portStr, 10);
		if (!isNaN(port) && port > 0 && port <= 65535) {
			return { host, port };
		}
	}
	// 仅域名/IP，使用默认 443 端口
	return { host: proxyStr, port: 443 };
}

/**
 * 其他小型工具函数
 */
function formatIdentifier(arr, offset = 0) {
	const hex = [...arr.slice(offset, offset + 16)].map(b => b.toString(16).padStart(2, '0')).join('');
	return `${hex.substring(0,8)}-${hex.substring(8,12)}-${hex.substring(12,16)}-${hex.substring(16,20)}-${hex.substring(20)}`;
}

function base64ToArray(b64Str) {
	if (!b64Str) return { error: null };
	try {
		const binaryString = atob(b64Str.replace(/-/g, '+').replace(/_/g, '/'));
		const bytes = new Uint8Array(binaryString.length);
		for (let i = 0; i < binaryString.length; i++) {
			bytes[i] = binaryString.charCodeAt(i);
		}
		return { earlyData: bytes.buffer, error: null };
	} catch (error) {
		return { error };
	}
}

function closeSocketQuietly(socket) {
	try {
		if (socket && (socket.readyState === 1 || socket.readyState === 2)) socket.close();
	} catch (error) {}
}


// ==================================================================================
// HTML 页面内容
// ==================================================================================

function handleHomePage(request) {
	const url = new URL(request.url);
	const currentDomain = url.hostname;
	const body = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Snippets</title><style>body{font-family:Arial,sans-serif;margin:0;padding:40px 20px;background:linear-gradient(135deg,#667eea 0%,#18800e 100%);min-height:100vh;display:flex;align-items:center;justify-content:center}.container{max-width:600px;background:#fff;padding:40px;border-radius:10px;box-shadow:0 10px 40px rgba(0,0,0,.3);text-align:center}h1{color:#667eea;margin-bottom:20px}.info{font-size:18px;color:#666;margin:20px 0}.link{display:inline-block;background:#667eea;color:#fff;padding:12px 30px;border-radius:5px;text-decoration:none;margin-top:20px}.link:hover{background:#5568d3}.footer{margin-top:30px;padding-top:20px;border-top:1px solid #eee;font-size:14px;color:#999}.footer a{color:#667eea;text-decoration:none;margin:0 10px}.footer a:hover{text-decoration:underline}</style></head><body><div class="container"><h1>Hello Snippets</h1><div class="info">请访问: <strong>https://${currentDomain}/${yourUUID}</strong><br><br>查看订阅和使用说明</div><div class="footer"><a href="https://github.com/eooce/CF-Workers-and-Snip-VLESS" target="_blank">GitHub</a>|<a href="https://t.me/eooceu" target="_blank">TG群组</a></div></div></body></html>`;
	return new Response(body, {
		headers: {
			'Content-Type': 'text/html; charset=utf-8',
			'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
		},
	});
}

function handleSubPage(request) {
	const url = new URL(request.url);
	const currentDomain = url.hostname;
	const v2raySubLink = `https://${currentDomain}/sub/${yourUUID}`;
	const clashSubLink = `https://sublink.eooce.com/clash?config=https://${currentDomain}/sub/${yourUUID}`;
	const singboxSubLink = `https://sublink.eooce.com/singbox?config=https://${currentDomain}/sub/${yourUUID}`;
	const body = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>订阅链接</title><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:Arial,sans-serif;background:linear-gradient(135deg,#667eea 0%,#18800e 100%);min-height:100vh;padding:20px}.container{max-width:900px;margin:0 auto;background:#fff;border-radius:15px;padding:30px;box-shadow:0 20px 60px rgba(0,0,0,.3)}h1{color:#667eea;margin-bottom:10px;font-size:2rem;text-align:center}.section{margin-bottom:25px}.section-title{color:#667eea;font-size:16px;font-weight:600;margin-bottom:12px;padding-bottom:6px;border-bottom:2px solid #667eea}.link-box{background:#f7f9fc;border:1px solid #e1e8ed;border-radius:8px;padding:12px;margin-bottom:10px}.link-label{font-size:16px;color:#666;margin-bottom:6px;font-weight:700}.link-content{display:flex;gap:8px}.link-text{flex:1;background:#fff;padding:8px 12px;border-radius:5px;border:1px solid #ddd;font-size:.8rem;word-break:break-all;font-family:monospace}.copy-btn{background:#667eea;color:#fff;border:none;padding:8px 16px;border-radius:5px;cursor:pointer;font-size:13px;white-space:nowrap}.copy-btn:hover{background:#5568d3}.copy-btn.copied{background:#48c774}.usage-section{background:#fff9e6;border-left:4px solid #ffc107;padding:15px;border-radius:5px;margin-top:25px}.usage-title{color:#f57c00;font-size:1.2rem;font-weight:600;margin-bottom:12px}.usage-item{margin-bottom:12px;font-size:13px;line-height:1.6}.usage-item strong{color:#333;display:block;margin-bottom:4px}.usage-item code{background:#fff;padding:2px 6px;border-radius:3px;color:#e91e63;font-size:13px;border:1px solid #ddd;word-wrap:break-word;word-break:break-all;display:inline-block;max-width:100%}.example{color:#666;font-size:14px;margin-left:8px}.footer{margin-top:30px;padding-top:20px;border-top:1px solid #e1e8ed;text-align:center;font-size:14px;color:#999}.footer a{color:#667eea;text-decoration:none;margin:0 10px}.footer a:hover{text-decoration:underline}@media (max-width:768px){.container{padding:20px}.link-content{flex-direction:column}.copy-btn{width:100%}}</style></head><body><div class="container"><h1>Snippets 订阅中心</h1><div class="section"><div class="section-title">🔗 通用订阅</div><div class="link-box"><div class="link-label">v2rayN / Loon / Shadowrocket / Karing</div><div class="link-content"><div class="link-text" id="v2ray-link">${v2raySubLink}</div><button class="copy-btn" onclick="copyToClipboard('v2ray-link',this)">复制</button></div></div></div><div class="section"><div class="section-title">😺 Clash 系列订阅</div><div class="link-box"><div class="link-label">Mihomo / FlClash / Clash Meta</div><div class="link-content"><div class="link-text" id="clash-link">${clashSubLink}</div><button class="copy-btn" onclick="copyToClipboard('clash-link',this)">复制</button></div></div></div><div class="section"><div class="section-title">📦 Sing-box 系列订阅</div><div class="link-box"><div class="link-label">Sing-box / SFI / SFA</div><div class="link-content"><div class="link-text" id="singbox-link">${singboxSubLink}</div><button class="copy-btn" onclick="copyToClipboard('singbox-link',this)">复制</button></div></div></div><div class="usage-section"><div class="usage-title">⚙️ 自定义路径(节点里的path)使用说明</div><div class="usage-item"><strong>1. 默认路径</strong><code>/?ed=2560</code><div class="example">使用代码里设置的默认proxyip</div></div><div class="usage-item"><strong>2. 带端口的proxyip</strong><code>/?ed=2560&proxyip=210.61.97.241:81</code><br><code>/?ed=2560&proxyip=proxy.xxxxxxxx.tk:50001</code></div><div class="usage-item"><strong>3. 域名proxyip</strong><code>/?ed=2560&proxyip=ProxyIP.SG.CMLiussss.net</code></div><div class="usage-item"><strong>4. [暂不支持] SOCKS5 代理</strong>（此版本的脚本简化了逻辑，不支持SOCKS/HTTP代理作为proxyip）</div></div><div class="footer"><a href="https://github.com/eooce/CF-Workers-and-Snip-VLESS" target="_blank">GitHub 项目</a>|<a href="https://t.me/eooceu" target="_blank">Telegram 群组</a>|<a href="https://check-proxyip.ssss.nyc.mn" target="_blank">ProxyIP 检测服务</a></div></div><script>function copyToClipboard(e,t){const n=document.getElementById(e).textContent;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(n).then(()=>{showCopySuccess(t)}).catch(()=>{fallbackCopy(n,t)}):fallbackCopy(n,t)}function fallbackCopy(e,t){const n=document.createElement("textarea");n.value=e,n.style.position="fixed",n.style.left="-999999px",document.body.appendChild(n),n.select();try{document.execCommand("copy"),showCopySuccess(t)}catch(e){alert("复制失败，请手动复制")}document.body.removeChild(n)}function showCopySuccess(e){const t=e.textContent;e.textContent="已复制",e.classList.add("copied"),setTimeout(()=>{e.textContent=t,e.classList.remove("copied")},2e3)}</script></body></html>`;
	return new Response(body, {
		headers: {
			'Content-Type': 'text/html; charset=utf-8',
			'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
		},
	});
}
