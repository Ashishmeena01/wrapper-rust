param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8080
)

Write-Host "Building native high-performance RPS test runner..." -ForegroundColor Cyan
cargo build --release --bin rps_test

Write-Host "Executing progressive RPS saturation test against http://${HostName}:${Port}..." -ForegroundColor Green
.\target\release\rps_test.exe $HostName $Port
