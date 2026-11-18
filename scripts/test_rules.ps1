param (
    [string]$BaseUrl = "http://localhost:8080"
)

Write-Host "==> Testing GET /health at $BaseUrl/health"
try {
    $health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get
    Write-Host "Health status: $($health.status)"
    if ($health.status -ne "ok") {
        Write-Error "Health check did not return status ok"
        exit 1
    }
} catch {
    Write-Error "Failed to reach health endpoint: $_"
    exit 1
}

Write-Host "`n==> Testing Product Creation POST /products"
$prodId = "ptest_" + (Get-Random)
$prodBody = @{
    id = $prodId
    name = "Test Smartphone"
    category = "electronics"
    price = 49999
    stock = 1
} | ConvertTo-Json

$created = Invoke-RestMethod -Uri "$BaseUrl/products" -Method Post -Body $prodBody -ContentType "application/json"
Write-Host "Created product: $($created.id) with stock: $($created.stock)"

Write-Host "`n==> Testing Last Unit Concurrency (50 parallel buyers for 1 unit)"
$results = 1..50 | ForEach-Object -Parallel {
    $uid = "u_parallel_$_"
    $pid = $using:prodId
    $url = "$using:BaseUrl/orders"
    $body = @{ userId = $uid; productId = $pid } | ConvertTo-Json
    try {
        $resp = Invoke-WebRequest -Uri $url -Method Post -Body $body -ContentType "application/json" -SkipHttpErrorCheck
        $resp.StatusCode
    } catch {
        $_.Exception.Response.StatusCode.value__
    }
} -ThrottleLimit 50

$grouped = $results | Group-Object | Select-Object Name, Count
Write-Host "Order status results:"
$grouped | Format-Table -AutoSize

$finalProd = Invoke-RestMethod -Uri "$BaseUrl/products/$prodId" -Method Get
Write-Host "Final product stock: $($finalProd.stock)"

Write-Host "`n==> Tests completed!"
