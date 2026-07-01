# create_strategies.ps1 — Windows-native strategy creator (HTTP only).
# Reuses the values you already pasted in config.sh (no retyping).
#
# NOTE: set GATEWAY_URL in config.sh to the ACTUAL gateway host reachable from
# this machine (localhost only works if the gateway runs here). The news-driven
# audit cases (qty/value/dpr/velocity/trade) still need the Linux box (Kafka/Redis).
#
# Run:  powershell -ExecutionPolicy Bypass -File .\create_strategies.ps1
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

# Parse KEY='value' lines out of config.sh.
$cfg = @{}
Get-Content (Join-Path $here 'config.sh') | ForEach-Object {
  if ($_ -match "^\s*([A-Za-z_]\w*)='([^']*)'") { $cfg[$matches[1]] = $matches[2] }
}
$GATEWAY = $cfg['GATEWAY_URL']; $TOKEN = $cfg['ACCESS_TOKEN']; $APP = $cfg['APP_ID']
$CID = $cfg['CLIENT_ID']; $SRC = $cfg['SOURCE']; $MODE = $cfg['TRADING_MODE']

if (-not $TOKEN -or $TOKEN -like 'PASTE_*') { Write-Error 'Set ACCESS_TOKEN in config.sh'; exit 1 }
$headers = @{ Authorization = "Bearer $TOKEN"; appId = $APP; userId = $CID; source = $SRC }

function New-Strategy($name, $qty, $cat, $desc) {
  $body = @{
    user_id = $CID; strategy_name = $name; description = $desc
    trading_mode = $MODE; activate_immediately = $true
    conditions = @{ match_all_news = $false; impact_score_min = 0; impact_score_max = 10;
      sentiments = @(); categories = @($cat); market_cap_types = @(); exchanges = @('NSE') }
    trade_config = @{ order_type = 'LIMIT'; product_type = 'INTRADAY'; validity = 'DAY';
      quantity = $qty; exchange = 'NSE'; order_side = 'BUY'; stop_loss_pct = 1.0; take_profit_pct = 2.0;
      stop_loss_type = 'FIXED'; take_profit_type = 'FIXED'; trade_window_start = ''; trade_window_end = '' }
    risk_limits = @{ max_daily_trades = 1000; max_per_trade_risk = 100000; max_loss_per_day = 100000000;
      enable_risk_checks = $true; enable_auto_square_off = $true; auto_square_off_time = '15:05';
      position_sizing = 'FIXED'; max_amount_per_stock = 0; max_trades_per_strategy = 1000 }
  } | ConvertTo-Json -Depth 6
  Write-Host "Creating $name (qty=$qty, category=$cat, mode=$MODE)..."
  try {
    $r = Invoke-RestMethod -Uri "$GATEWAY/api/v1/strategies" -Method Post -Headers $headers -Body $body -ContentType 'application/json'
    Write-Host "  OK: $($r | ConvertTo-Json -Depth 4 -Compress)"
  } catch {
    $resp = $_.Exception.Response
    if ($resp) {
      $code = [int]$resp.StatusCode
      $bodyText = ''
      try { $sr = New-Object System.IO.StreamReader($resp.GetResponseStream()); $bodyText = $sr.ReadToEnd() } catch {}
      Write-Host "  ERROR ${code}: $bodyText"
    } else { Write-Host "  ERROR: $($_.Exception.Message)" }
  }
}

Write-Host "Gateway=$GATEWAY  Client=$CID  Mode=$MODE"
New-Strategy 'AUDIT-TRADE'    1     'AUDIT_TRADE'    'passes all checks; one order placed (token 2885 RELIANCE)'
New-Strategy 'AUDIT-DPR'      10    'AUDIT_DPR'      'DPR_UPPER_BREACH (token 22 ACC, LTP seeded above dpr_upper)'
New-Strategy 'AUDIT-QTY'      60000 'AUDIT_QTY'      'QTY_LIMIT_EXCEEDED (token 2885 RELIANCE)'
New-Strategy 'AUDIT-VALUE'    50000 'AUDIT_VALUE'    'ORDER_VALUE_LIMIT_EXCEEDED (token 2885 RELIANCE)'
New-Strategy 'AUDIT-EXPOSURE' 1000  'AUDIT_EXPOSURE' 'EXPOSURE_LIMIT_EXCEEDED on 2nd order (tokens 21690 DIXON + 25 ADANIENT)'
New-Strategy 'AUDIT-BAN'      1     'AUDIT_BAN'      'BANNED_TOKEN (token 14366 IDEA, in BANNED_TOKENS env)'
Write-Host ""
Write-Host "Strategies created. Watch STRATEGY_INITIALIZED in rules-engine logs."
Write-Host "Now run:  powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1 [qty|value|dpr|exposure|ban|trade|all]"
