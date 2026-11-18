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
$jobs = 1..50 | ForEach-Object {
    $uid = "u_parallel_$_"
    $targetProdId = $prodId
    $url = "$BaseUrl/orders"
    Start-Job -ScriptBlock {
        param($url, $uid, $targetProdId)
        $body = @{ userId = $uid; productId = $targetProdId } | ConvertTo-Json
        try {
            $resp = Invoke-WebRequest -Uri $url -Method Post -Body $body -ContentType "application/json" -UseBasicParsing
            $resp.StatusCode
        } catch {
            $_.Exception.Response.StatusCode.value__
        }
    } -ArgumentList $url, $uid, $targetProdId
}

$results = $jobs | Wait-Job | Receive-Job
$jobs | Remove-Job
$grouped = $results | Group-Object | Select-Object Name, Count
Write-Host "Order status results:"
$grouped | Format-Table -AutoSize


$finalProd = Invoke-RestMethod -Uri "$BaseUrl/products/$prodId" -Method Get
Write-Host "Final product stock: $($finalProd.stock)"

Write-Host "`n==> Running Test 2: Double-Click Test (20 concurrent requests for same user & product)"
$clickProdId = "pclick_" + (Get-Random)
$clickProdBody = @{
    id = $clickProdId
    name = "Double-Click Item"
    category = "electronics"
    price = 1000
    stock = 1000
} | ConvertTo-Json

$createdClick = Invoke-RestMethod -Uri "$BaseUrl/products" -Method Post -Body $clickProdBody -ContentType "application/json"
Write-Host "Created test product: $($createdClick.id) with stock: $($createdClick.stock)"

$sameUserId = "u_click_" + (Get-Random)
$clickJobs = 1..20 | ForEach-Object {
    $uid = $sameUserId
    $targetProdId = $clickProdId
    $url = "$BaseUrl/orders"
    Start-Job -ScriptBlock {
        param($url, $uid, $targetProdId)
        $body = @{ userId = $uid; productId = $targetProdId } | ConvertTo-Json
        try {
            $resp = Invoke-WebRequest -Uri $url -Method Post -Body $body -ContentType "application/json" -UseBasicParsing
            $resp.StatusCode
        } catch {
            $_.Exception.Response.StatusCode.value__
        }
    } -ArgumentList $url, $uid, $targetProdId
}

$clickResults = $clickJobs | Wait-Job | Receive-Job
$clickJobs | Remove-Job
$clickGrouped = $clickResults | Group-Object | Select-Object Name, Count
Write-Host "Double-click status results:"
$clickGrouped | Format-Table -AutoSize

$clickFinalProd = Invoke-RestMethod -Uri "$BaseUrl/products/$clickProdId" -Method Get
Write-Host "Product stock after double-click test: $($clickFinalProd.stock) (Expected: 999)"

$userOrders = Invoke-RestMethod -Uri "$BaseUrl/users/$sameUserId/orders" -Method Get
Write-Host "User order count: $($userOrders.orders.Count) (Expected: 1)"

Write-Host "`n==> All tests completed!"
