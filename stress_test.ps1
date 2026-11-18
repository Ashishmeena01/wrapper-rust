# ==============================================================================
# Last Server Standing — Native Progressive Saturation Benchmark (PowerShell)
#
# Progressively ramps load:
# - Stage 1:  1,000 requests
# - Stage 2:  5,000 requests
# - Stage 3+: Multiples of 4,000 (8,000 -> 12,000 -> 16,000 -> 20,000 -> 24,000...)
#
# Stops automatically when the server stops handling requests or error rate > 2%.
# ==============================================================================

param(
    [string]$HostName = "",
    [int]$Port = 0,
    [int]$Concurrency = 40
)

Write-Host "`n======================================================================" -ForegroundColor Cyan
Write-Host "      LAST SERVER STANDING — NATIVE PROGRESSIVE STRESS TEST           " -ForegroundColor Cyan
Write-Host "======================================================================`n" -ForegroundColor Cyan

if (-not $HostName) {
    $inputHost = Read-Host "Enter Server Host [default: 127.0.0.1]"
    $HostName = if ($inputHost) { $inputHost.Trim() } else { "127.0.0.1" }
}

if ($Port -eq 0) {
    $inputPort = Read-Host "Enter Server Port [default: 8080]"
    $Port = if ($inputPort) { [int]$inputPort } else { 8080 }
}

$BaseUrl = "http://${HostName}:${Port}"

Write-Host "`n==> Checking server health at $BaseUrl/health..." -ForegroundColor Cyan
try {
    $health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get -TimeoutSec 3
    if ($health.status -ne "ok") {
        Write-Host "Error: Server returned unexpected health status: $($health.status)" -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ Server is HEALTHY (200 OK)`n" -ForegroundColor Green
} catch {
    Write-Host "Error: Could not reach server at $BaseUrl/health: $_" -ForegroundColor Red
    Write-Host "Make sure your server is running ('cargo run --release')."
    exit 1
}

# Seed products
Write-Host "==> Seeding catalog dataset for stress testing..." -ForegroundColor Cyan
$seedTime = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$flashPid = "stress_flash_$seedTime"
$regPid = "stress_reg_$seedTime"

$flashBody = @{
    id = $flashPid
    name = "Flash Item"
    category = "smartphones"
    price = 99999
    stock = 50000
} | ConvertTo-Json

$regBody = @{
    id = $regPid
    name = "Catalog Item"
    category = "accessories"
    price = 1500
    stock = 100000
} | ConvertTo-Json

Invoke-RestMethod -Uri "$BaseUrl/products" -Method Post -Body $flashBody -ContentType "application/json" | Out-Null
Invoke-RestMethod -Uri "$BaseUrl/products" -Method Post -Body $regBody -ContentType "application/json" | Out-Null
Write-Host "✓ Created test products: $flashPid and $regPid`n" -ForegroundColor Green

# Define stages: 1000, 5000, 8000, 12000, 16000, 20000...
$stages = @(1000, 5000)
for ($s = 8000; $s -le 80000; $s += 4000) {
    $stages += $s
}

Write-Host "Starting Progressive Load Stages (Concurrency: $Concurrency workers):" -ForegroundColor Yellow
$header = "{0,-10} | {1,-12} | {2,-16} | {3,-12} | {4,-10} | {5}" -f "Stage", "Requests", "Throughput", "Duration", "Error %", "Status"
Write-Host $header
Write-Host ("-" * 86)

$peakRps = 0.0
$highestSuccess = 0
$stageNum = 1

foreach ($reqs in $stages) {
    $perWorker = [math]::Floor($reqs / $Concurrency)
    $remainder = $reqs % $Concurrency

    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    $jobs = 0..($Concurrency - 1) | ForEach-Object {
        $wId = $_
        $count = $perWorker + $(if ($wId -lt $remainder) { 1 } else { 0 })
        
        Start-Job -ScriptBlock {
            param($url, $flashId, $regId, $workerId, $n)
            $ok = 0
            $err = 0
            $five = 0

            for ($i = 1; $i -le $n; $i++) {
                $roll = (Get-Random -Minimum 0 -Maximum 100)
                $code = 0

                try {
                    if ($roll -lt 60) {
                        # 60% GET /products/:id
                        $resp = Invoke-WebRequest -Uri "$url/products/$flashId" -Method Get -UseBasicParsing
                        $code = $resp.StatusCode
                    } elseif ($roll -lt 75) {
                        # 15% GET /products?category=...
                        $resp = Invoke-WebRequest -Uri "$url/products?category=smartphones&page=1&limit=20" -Method Get -UseBasicParsing
                        $code = $resp.StatusCode
                    } elseif ($roll -lt 95) {
                        # 20% POST /orders
                        $b = @{ userId = "u_w${workerId}_$($i % 50)"; productId = $flashId } | ConvertTo-Json -Compress
                        $resp = Invoke-WebRequest -Uri "$url/orders" -Method Post -Body $b -ContentType "application/json" -UseBasicParsing
                        $code = $resp.StatusCode
                    } else {
                        # 5% GET /users/:id/orders
                        $resp = Invoke-WebRequest -Uri "$url/users/u_w${workerId}_$($i % 50)/orders" -Method Get -UseBasicParsing
                        $code = $resp.StatusCode
                    }
                } catch {
                    if ($_.Exception.Response) {
                        $code = [int]$_.Exception.Response.StatusCode
                    } else {
                        $code = 0
                    }
                }

                if ($code -in 200, 201, 400, 404, 409) {
                    $ok++
                } else {
                    $err++
                    if ($code -ge 500) { $five++ }
                }
            }

            [PSCustomObject]@{ Ok = $ok; Err = $err; Five = $five }
        } -ArgumentList $BaseUrl, $flashPid, $regPid, $wId, $count
    }

    $results = $jobs | Wait-Job | Receive-Job
    $jobs | Remove-Job

    $sw.Stop()
    $elapsedSec = [math]::Max(0.01, $sw.Elapsed.TotalSeconds)
    $rps = [math]::Round($reqs / $elapsedSec, 1)

    $totalOk = ($results | Measure-Object -Property Ok -Sum).Sum
    $totalErr = ($results | Measure-Object -Property Err -Sum).Sum
    $totalFive = ($results | Measure-Object -Property Five -Sum).Sum

    $errPct = [math]::Round(($totalErr * 100.0) / $reqs, 2)
    $failed = ($errPct -gt 2.0) -or ($totalOk -eq 0) -or ($totalFive -gt 0)

    if ($failed) {
        $statusStr = "FAILED"
        $statusColor = "Red"
    } else {
        $statusStr = "PASSED"
        $statusColor = "Green"
        $highestSuccess = $reqs
        if ($rps -gt $peakRps) { $peakRps = $rps }
    }

    $line = "#{0,-9} | {1,-12:N0} | {2,11:N1} req/s | {3,9:N2} s | {4,8:N2}% | {5}" -f `
        $stageNum, $reqs, $rps, $elapsedSec, $errPct, $statusStr

    Write-Host $line -ForegroundColor $statusColor

    if ($failed) {
        Write-Host "`n==> SERVER SATURATION REACHED AT $reqs REQUESTS!" -ForegroundColor Red
        Write-Host "    Error Rate: $errPct% (5xx: $totalFive, Failed/Dropped: $totalErr)"
        Write-Host "    Highest Sustained Successful Load: $highestSuccess requests" -ForegroundColor Yellow
        Write-Host "    Peak Throughput Observed:          $peakRps req/sec`n" -ForegroundColor Green
        break
    }

    $stageNum++
    Start-Sleep -Milliseconds 500
}

if (-not $failed) {
    Write-Host "`n==> ALL STAGES PASSED! Server handled up to $highestSuccess requests seamlessly!" -ForegroundColor Green
    Write-Host "    Peak Throughput Observed: $peakRps req/sec`n" -ForegroundColor Green
}
