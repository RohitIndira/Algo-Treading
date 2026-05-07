# Binary WebSocket - Frontend Integration

## Connect

```
ws://YOUR_HOST:8080/enhanced-stream?tokens=476,11536&format=binary&client_id=dashboard_1
```

| Param | Required | Description |
|-------|----------|-------------|
| `format` | yes | Set to `binary` (default is json) |
| `tokens` | pick one | Comma-separated token numbers (fastest) |
| `stocks` | pick one | Comma-separated symbols e.g. `RELIANCE,TCS` |
| `all` | pick one | `true` to subscribe to all ~5000+ stocks |
| `client_id` | no | Persistent ID for reconnection |

## Setup

```javascript
const ws = new WebSocket('ws://HOST:8080/enhanced-stream?tokens=476,11536&format=binary');
ws.binaryType = 'arraybuffer'; // MUST set this before any messages arrive
```

## Message Types (first byte)

| Byte | Type |
|------|------|
| `0x01` | Market Data |
| `0x02` | Welcome |
| `0x04` | Ping |
| `0x05` | Pong |
| `0x06` | Error |
| `0x08` | Market Status |
| `0x0A` | Subscription Ack |

Check the first byte to route handling:

```javascript
ws.onmessage = (event) => {
    const view = new DataView(event.data);
    const type = view.getUint8(0);

    switch (type) {
        case 0x01: handleMarketData(event.data); break;
        case 0x02: console.log('Welcome received'); break;
        case 0x05: /* pong - ignore */; break;
        case 0x06: handleError(event.data); break;
    }
};
```

## Decode Market Data (0x01)

All multi-byte values are **little-endian**.

| Offset | Field | Type | Read With |
|--------|-------|------|-----------|
| 0 | msg type | uint8 | `getUint8(0)` |
| 1 | timestamp | int64 | `getBigInt64(1, true)` |
| 9 | token len (N) | uint8 | `getUint8(9)` |
| 10 | token | string[N] | TextDecoder |
| 10+N | symbol len (M) | uint8 | `getUint8(10+N)` |
| 11+N | symbol | string[M] | TextDecoder |
| 11+N+M | exchange | uint8 | 0=NSE 1=BSE 2=NFO 3=MCX |
| 12+N+M | ltp | float32 | `getFloat32(_, true)` |
| 16+N+M | open | float32 | |
| 20+N+M | high | float32 | |
| 24+N+M | low | float32 | |
| 28+N+M | close | float32 | |
| 32+N+M | prev_close | float32 | |
| 36+N+M | volume | int64 | `getBigInt64(_, true)` |
| 44+N+M | percent_change | float32 | |
| 48+N+M | week_52_high | float32 | |
| 52+N+M | week_52_low | float32 | |
| 56+N+M | day_high | float32 | |
| 60+N+M | day_low | float32 | |
| 64+N+M | is_new_52w_high | uint8 | 1 = yes |
| 65+N+M | is_new_52w_low | uint8 | 1 = yes |

## Copy-Paste Decoder

```javascript
function decodeBinaryMarketData(buffer) {
    const view = new DataView(buffer);
    let o = 1; // skip msg type byte

    const timestamp = Number(view.getBigInt64(o, true)); o += 8;

    const tLen = view.getUint8(o); o += 1;
    const token = new TextDecoder().decode(new Uint8Array(buffer, o, tLen)); o += tLen;

    const sLen = view.getUint8(o); o += 1;
    const symbol = new TextDecoder().decode(new Uint8Array(buffer, o, sLen)); o += sLen;

    const exchange = ['NSE', 'BSE', 'NFO', 'MCX'][view.getUint8(o)]; o += 1;

    const f32 = () => { const v = view.getFloat32(o, true); o += 4; return v; };
    const i64 = () => { const v = Number(view.getBigInt64(o, true)); o += 8; return v; };

    return {
        timestamp, token, symbol, exchange,
        ltp: f32(), open: f32(), high: f32(), low: f32(),
        close: f32(), prevClose: f32(),
        volume: i64(),
        percentChange: f32(),
        week52High: f32(), week52Low: f32(),
        dayHigh: f32(), dayLow: f32(),
        isNew52wHigh: view.getUint8(o++) === 1,
        isNew52wLow: view.getUint8(o++) === 1,
    };
}
```

## Keep-Alive (Ping)

Send a ping every 30s to keep the connection alive:

```javascript
setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(new Uint8Array([0x04])); // ping byte
    }
}, 30000);
```

Server responds with `0x05` (pong). No need to handle it.

## React Hook Example

```javascript
function useBinaryStream(tokens) {
    const [prices, setPrices] = useState({});

    useEffect(() => {
        const ws = new WebSocket(
            `ws://${location.hostname}:8080/enhanced-stream?tokens=${tokens.join(',')}&format=binary`
        );
        ws.binaryType = 'arraybuffer';

        ws.onmessage = (e) => {
            const view = new DataView(e.data);
            if (view.getUint8(0) !== 0x01) return;

            const data = decodeBinaryMarketData(e.data);
            setPrices(prev => ({ ...prev, [data.token]: data }));
        };

        const ping = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.send(new Uint8Array([0x04]));
        }, 30000);

        return () => { clearInterval(ping); ws.close(); };
    }, [tokens.join(',')]);

    return prices;
}

// Usage
const prices = useBinaryStream(['476', '11536', '1594']);
```

## Common Mistakes

| Problem | Fix |
|---------|-----|
| Getting gibberish / Blob | Set `ws.binaryType = 'arraybuffer'` **before** connection opens |
| Prices look wrong | Use `true` (little-endian) in all `getFloat32`/`getBigInt64` calls |
| Connection drops silently | Add the ping interval above |
| Wrong symbol/token | Offsets shift with string lengths - use the decoder function as-is, don't hardcode offsets |

## Message Sizes

| Format | Market Data | Savings |
|--------|-------------|---------|
| JSON | ~450 bytes | - |
| Binary | ~75-95 bytes | **~80%** |

At 10 updates/sec per stock, binary saves ~3.5 KB/s per stock vs JSON.
