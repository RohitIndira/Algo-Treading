const http = require('http');
const https = require('https');
const url = require('url');

const PORT = 3001;
const TARGET_HOST = 'livemiddleware.indiratrade.com';
const TOKEN_PATH = '/order-notify/ws/createWsToken';

const server = http.createServer((req, res) => {
    // CORS headers for all responses
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    // Handle preflight
    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    const parsed = url.parse(req.url, true);

    // Health check
    if (parsed.pathname === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', proxy: 'running' }));
        return;
    }

    // Proxy the token request
    if (parsed.pathname === '/api/ws-token') {
        const sessionToken = parsed.query.token;
        const isSso = parsed.query.sso === 'true';

        if (!sessionToken) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status: 'NOT_OK', message: 'Missing token parameter' }));
            return;
        }

        const headers = {
            'Authorization': `Bearer ${sessionToken}`,
            'Content-Type': 'application/json'
        };
        if (isSso) headers['sso'] = 'True';

        const options = {
            hostname: TARGET_HOST,
            port: 443,
            path: TOKEN_PATH,
            method: 'GET',
            headers
        };

        console.log(`[${new Date().toLocaleTimeString()}] Proxying token request for session: ${sessionToken.substring(0, 12)}...`);

        const proxyReq = https.request(options, (proxyRes) => {
            let body = '';
            proxyRes.on('data', (chunk) => { body += chunk; });
            proxyRes.on('end', () => {
                console.log(`[${new Date().toLocaleTimeString()}] Response: ${proxyRes.statusCode} - ${body.substring(0, 100)}`);
                res.writeHead(proxyRes.statusCode, { 'Content-Type': 'application/json' });
                res.end(body);
            });
        });

        proxyReq.on('error', (err) => {
            console.error(`[${new Date().toLocaleTimeString()}] Proxy error:`, err.message);
            res.writeHead(502, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status: 'NOT_OK', message: `Proxy error: ${err.message}` }));
        });

        proxyReq.end();
        return;
    }

    // 404 for anything else
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'NOT_OK', message: 'Not found' }));
});

server.listen(PORT, () => {
    console.log(`\n  CodiFi CORS Proxy Server`);
    console.log(`  ========================`);
    console.log(`  Running on: http://localhost:${PORT}`);
    console.log(`  Health:     http://localhost:${PORT}/health`);
    console.log(`  Token API:  http://localhost:${PORT}/api/ws-token?token=<session_token>`);
    console.log(`\n  Open order-status-poc.html in browser and use "Auto (via Proxy)" mode.\n`);
});
