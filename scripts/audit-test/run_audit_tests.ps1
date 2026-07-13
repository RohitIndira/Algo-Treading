# run_audit_tests.ps1 - Windows audit test runner
# Tokens used: 2885=RELIANCE, 22=ACC, 21690=DIXON, 25=ADANIENT, 14366=IDEA
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 qty
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 value
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 dpr
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 exposure
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 ban
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 trade
#   powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 all

$ErrorActionPreference = 'Stop'
$here   = Split-Path -Parent $MyInvocation.MyCommand.Path
$pubDir = Join-Path $here 'kafka_publisher'

# Parse config.sh
$cfg = @{}
Get-Content (Join-Path $here 'config.sh') | ForEach-Object {
    if ($_ -match "^\s*([A-Za-z_]\w*)='([^']*)'") { $cfg[$Matches[1]] = $Matches[2] }
}
$BROKER         = $cfg['KAFKA_BROKER']
$TOPIC          = $cfg['TOPIC']
$REDIS_HOST     = $cfg['REDIS_HOST']
$REDIS_PASSWORD = $cfg['REDIS_PASSWORD']
$REDIS_PORT     = if ($cfg['REDIS_PORT'])   { [int]$cfg['REDIS_PORT']   } else { 6379 }
$REDIS_DB       = if ($cfg['REDIS_DB'])     { [int]$cfg['REDIS_DB']     } else { 1 }
$TICKSTORE_DB   = if ($cfg['TICKSTORE_DB']) { [int]$cfg['TICKSTORE_DB'] } else { 1 }
$REAL_TOKEN     = $cfg['REAL_TOKEN']
$REAL_SYMBOL    = $cfg['REAL_SYMBOL']
$EXCH         = 'nse'

$pubExe = Join-Path $pubDir 'kafka_publisher.exe'
if (-not (Test-Path $pubExe)) {
    Write-Host 'Building kafka_publisher.exe...'
    $env:GOWORK = 'off'
    Push-Location $pubDir
    go build -o kafka_publisher.exe .
    Pop-Location
}

# Publish news JSON via stdin to avoid PS5.1 quote-stripping on native commands
function Publish-News {
    param([string]$Token, [string]$Symbol, [string]$Category, [double]$Ltp)

    $nid = 'AUDIT-' + (Get-Date -Format 'yyyyMMddHHmmssfff')
    $dt  = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    $msg = '{"news_id":"' + $nid + '","newsid":"' + $nid + '","symbol":"' + $Symbol +
           '","exchange":"NSE","code":' + $Token + ',"token":' + $Token +
           ',"company":"INE000TEST01","companyname":"' + $Symbol +
           '","impact":"High","impact score":5,"sentiment":"POSITIVE","category":"' + $Category +
           '","mcap":1500000,"mcaptype":"Large","pct_change":0.5,"LastTradedPrice":' + $Ltp +
           ',"dt_tm":"' + $dt + '","document_date":"' + $dt + '"}'

    Write-Host ('  -> publishing  category=' + $Category + '  token=' + $Token)

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName               = $pubExe
    $psi.Arguments              = $BROKER + ' ' + $TOPIC
    $psi.UseShellExecute        = $false
    $psi.RedirectStandardInput  = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true

    $proc = [System.Diagnostics.Process]::Start($psi)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($msg)
    $proc.StandardInput.BaseStream.Write($bytes, 0, $bytes.Length)
    $proc.StandardInput.BaseStream.Flush()
    $proc.StandardInput.Close()

    $outTask = $proc.StandardOutput.ReadToEndAsync()
    $errTask = $proc.StandardError.ReadToEndAsync()
    $done    = $proc.WaitForExit(10000)

    $stdout = $outTask.Result.Trim()
    $stderr = $errTask.Result.Trim()

    if (-not $done -or $proc.ExitCode -ne 0) {
        Write-Host ('  PUBLISH FAILED: ' + $stderr)
    } else {
        Write-Host ('  ' + $stdout)
    }
}

# Redis via raw TCP with 3-second connect timeout
function Invoke-Redis {
    param([int]$Db, [string[]]$Cmd)

    $enc    = [System.Text.Encoding]::UTF8
    $client = New-Object System.Net.Sockets.TcpClient
    $task   = $client.ConnectAsync($REDIS_HOST, $REDIS_PORT)
    if (-not $task.Wait(3000)) {
        $client.Close()
        throw ('Redis connect timeout to ' + $REDIS_HOST + ':' + $REDIS_PORT)
    }
    $stream              = $client.GetStream()
    $stream.ReadTimeout  = 3000
    $stream.WriteTimeout = 3000

    function Send-Cmd([string[]]$parts) {
        $sb = '*' + $parts.Count + "`r`n"
        foreach ($p in $parts) {
            $sb += '$' + $enc.GetByteCount($p) + "`r`n" + $p + "`r`n"
        }
        $b = $enc.GetBytes($sb)
        $stream.Write($b, 0, $b.Length)
        $stream.Flush()
        $buf = New-Object byte[] 4096
        $n   = $stream.Read($buf, 0, $buf.Length)
        return $enc.GetString($buf, 0, $n)
    }

    if ($REDIS_PASSWORD) { Send-Cmd 'AUTH', $REDIS_PASSWORD | Out-Null }
    Send-Cmd 'SELECT', "$Db" | Out-Null
    $result = Send-Cmd $Cmd
    $client.Close()
    return $result
}

function Redis-Set    { param($Db, $Key, $Val) Invoke-Redis -Db $Db -Cmd 'SET',$Key,$Val    | Out-Null }
function Redis-Del    { param($Db, $Key)       Invoke-Redis -Db $Db -Cmd 'DEL',$Key         | Out-Null }
function Redis-RPush  { param($Db, $Key, $Val) Invoke-Redis -Db $Db -Cmd 'RPUSH',$Key,$Val  | Out-Null }
function Redis-Expire { param($Db, $Key, $Sec) Invoke-Redis -Db $Db -Cmd 'EXPIRE',$Key,$Sec | Out-Null }

# ---- Test cases ---------------------------------------------------------------

function Case-Qty {
    Write-Host ''
    Write-Host '[QTY] qty=60000 > 50000 -> expect ORDER_REJECTED QTY_LIMIT_EXCEEDED'
    Publish-News -Token $REAL_TOKEN -Symbol $REAL_SYMBOL -Category 'AUDIT_QTY' -Ltp 0
}

function Case-Value {
    Write-Host ''
    Write-Host '[VALUE] qty=50000 x LTP -> expect ORDER_REJECTED ORDER_VALUE_LIMIT_EXCEEDED'
    Publish-News -Token $REAL_TOKEN -Symbol $REAL_SYMBOL -Category 'AUDIT_VALUE' -Ltp 0
}

function Case-Trade {
    Write-Host ''
    Write-Host '[TRADE] qty=1 -> expect ORDER_AUDIT approved + broker call (LIVE = real order!)'
    Publish-News -Token $REAL_TOKEN -Symbol $REAL_SYMBOL -Category 'AUDIT_TRADE' -Ltp 0
}

function Case-Dpr {
    Write-Host ''
    Write-Host '[DPR] ACC (token 22) ltp=1610 above real dpr_upper 1598.6 -> expect DPR_UPPER_BREACH'
    # Uses ACC's REAL circuit band [1065.8, 1598.6]; ltp is bumped just above the
    # upper so the LIMIT order prices above the band. (The .sh version reads the
    # live band from Redis when present; PowerShell keeps ACC's known band static.)
    $ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $j  = '{"symbol":"ACC","token":"22","exchange":"nse","ltp":1610,"prev_close":1360,' +
          '"volume":1000,"timestamp":' + $ts + ',"tick_size":0.05,"percent_change":0.3,' +
          '"dpr_lower":1065.8,"dpr_upper":1598.6}'
    try {
        Redis-Set -Db $REDIS_DB -Key ('market:' + $EXCH + ':22') -Val $j
        Write-Host ('  Redis seeded  market:' + $EXCH + ':22  (ltp=1610, band=[1065.8,1598.6])')
    } catch {
        Write-Host ('  WARNING: Redis seed failed - ' + $_.Exception.Message)
    }
    Publish-News -Token '22' -Symbol 'ACC' -Category 'AUDIT_DPR' -Ltp 1610
}

function Case-Exposure {
    Write-Host ''
    Write-Host '[EXPOSURE] 2 orders x ~90L each, limit=1cr -> order1 PASSES, order2 EXPOSURE_LIMIT_EXCEEDED'
    Write-Host '  Tokens: 21690=DIXON (order1), 25=ADANIENT (order2)'

    $ts = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

    # Seed DIXON (21690) and ADANIENT (25) with LTP=9000 so qty=1000 -> 90L each
    $jDixon = '{"symbol":"DIXON","token":"21690","exchange":"nse","ltp":9000,"prev_close":8900,' +
              '"volume":500,"timestamp":' + $ts + ',"tick_size":1.0,"percent_change":0.5,' +
              '"dpr_lower":8000,"dpr_upper":10000}'
    $jAdani = '{"symbol":"ADANIENT","token":"25","exchange":"nse","ltp":9000,"prev_close":8900,' +
              '"volume":500,"timestamp":' + $ts + ',"tick_size":1.0,"percent_change":0.5,' +
              '"dpr_lower":8000,"dpr_upper":10000}'
    try {
        Redis-Set -Db $REDIS_DB -Key 'market:nse:21690' -Val $jDixon
        Redis-Set -Db $REDIS_DB -Key 'market:nse:25'    -Val $jAdani
        # Reset exposure counter so the test always starts clean
        Redis-Del -Db $REDIS_DB -Key 'exposure:ISPL19027'
        Write-Host '  Redis seeded  market:nse:21690 + market:nse:25  (ltp=9000, band=[8000,10000])'
        Write-Host '  Exposure key reset: exposure:ISPL19027'
    } catch {
        Write-Host ('  WARNING: Redis seed failed - ' + $_.Exception.Message)
    }

    Write-Host '  Order 1 (DIXON 21690): expect ORDER_AUDIT + published (exposure -> 90L)'
    Publish-News -Token '21690' -Symbol 'DIXON' -Category 'AUDIT_EXPOSURE' -Ltp 9000

    Write-Host '  Waiting 5 seconds before order 2...'
    Start-Sleep 5

    Write-Host '  Order 2 (ADANIENT 25): expect ORDER_AUDIT + EXPOSURE_LIMIT_EXCEEDED (90L+90L>1cr)'
    Publish-News -Token '25' -Symbol 'ADANIENT' -Category 'AUDIT_EXPOSURE' -Ltp 9000
}

function Case-Ban {
    Write-Host ''
    Write-Host '[BAN] token=14366 (IDEA) in BANNED_TOKENS -> expect ORDER_AUDIT + BANNED_TOKEN'
    $ts = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $j  = '{"symbol":"IDEA","token":"14366","exchange":"nse","ltp":8.5,"prev_close":8.3,' +
          '"volume":10000,"timestamp":' + $ts + ',"tick_size":0.05,"percent_change":0.2,' +
          '"dpr_lower":7.5,"dpr_upper":9.5}'
    try {
        Redis-Set -Db $REDIS_DB -Key 'market:nse:14366' -Val $j
        Write-Host '  Redis seeded  market:nse:14366  (ltp=8.5)'
    } catch {
        Write-Host ('  WARNING: Redis seed failed - ' + $_.Exception.Message)
    }
    Publish-News -Token '14366' -Symbol 'IDEA' -Category 'AUDIT_BAN' -Ltp 8.5
}

# ---- Run ---------------------------------------------------------------------

Write-Host ('Broker=' + $BROKER + '  Topic=' + $TOPIC + '  Redis=' + $REDIS_HOST + ':' + $REDIS_PORT)

$target = if ($args.Count -gt 0) { $args[0] } else { 'all' }
switch ($target) {
    'qty'      { Case-Qty }
    'value'    { Case-Value }
    'dpr'      { Case-Dpr }
    'exposure' { Case-Exposure }
    'ban'      { Case-Ban }
    'trade'    { Case-Trade }
    'all' {
        Case-Qty;      Start-Sleep 4
        Case-Value;    Start-Sleep 4
        Case-Dpr;      Start-Sleep 4
        Case-Exposure; Start-Sleep 10
        Case-Ban;      Start-Sleep 4
        Case-Trade
    }
    default {
        Write-Host 'usage: run_audit_tests.ps1 [qty|value|dpr|exposure|ban|trade|all]'
        exit 1
    }
}

Write-Host ''
Write-Host 'Done. Watch rules-engine for ORDER_AUDIT / ORDER_REJECTED.'
