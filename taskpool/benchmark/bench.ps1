# Runs BenchmarkPools on Windows and summarizes the results.
# See README.md, or run: powershell -ExecutionPolicy Bypass -File bench.ps1 -Help
param(
    # Scenarios, comma-separated (default: all).
    [string]$Scenarios = "",
    # Pools, comma-separated (default: all).
    [string]$Pools = "",
    # Runs of each benchmark.
    [int]$Count = 1,
    # -benchtime, e.g. 1s or 100000x.
    [string]$Benchtime = "1s",
    # GOMAXPROCS values, e.g. 4,8 (default: the machine's).
    [string]$Cpu = "",
    # Raw output file (default: results\<os>-<time>.txt).
    [string]$Out = "",
    # List the scenarios and pools.
    [switch]$List,
    [switch]$Help
)

$ErrorActionPreference = "Stop"
# -Out is relative to where the script was run from, not to its directory.
if ($Out -ne "") { $Out = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Out) }
Set-Location -Path $PSScriptRoot

$AllScenarios = "Handoff,ParallelTiny,LoopCPUShort,LoopCPULong,LoopBlocking,LoopMixed,Bursts"
$AllPools = "nbio,ants,gopool,fnet,fib-adaptive,fib-adaptive-chan,fib-elastic"

if ($Help) {
    @"
Usage: bench.ps1 [-Scenarios LIST] [-Pools LIST] [-Count N] [-Benchtime TIME]
                 [-Cpu LIST] [-Out FILE] [-List] [-Help]

  -Scenarios  $AllScenarios
  -Pools      $AllPools

Examples:
  .\bench.ps1
  .\bench.ps1 -Scenarios Handoff,Bursts -Pools nbio,fib-elastic -Count 3
"@
    exit 0
}
if ($List) {
    "scenarios: $AllScenarios"
    "pools:     $AllPools"
    exit 0
}

# Alternation turns a comma-separated list into an anchored regex group.
function Alternation([string]$list) { "^(" + ($list -replace ",", "|") + ")$" }

if ($Scenarios -eq "") { $Scenarios = $AllScenarios }
if ($Pools -eq "") { $Pools = $AllPools }
$filter = "BenchmarkPools/" + (Alternation $Scenarios) + "/" + (Alternation $Pools)

if ($Out -eq "") {
    $goos = (go env GOOS).Trim()
    $goarch = (go env GOARCH).Trim()
    $Out = Join-Path $PSScriptRoot ("results\$goos-$goarch-" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".txt")
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Out) | Out-Null

$flags = @("-run", '^$', "-bench", $filter, "-count", $Count, "-benchtime", $Benchtime, "-timeout", "0")
if ($Cpu -ne "") { $flags += @("-cpu", $Cpu) }

Write-Host "go test $($flags -join ' ')"
Write-Host "raw output: $Out"
# Tee-Object would write UTF-16 under Windows PowerShell 5.1, which summarize
# cannot read, so the lines go to a UTF-8 writer as they are shown.
$writer = [System.IO.StreamWriter]::new($Out)
try {
    go test @flags . | ForEach-Object { Write-Host $_; $writer.WriteLine($_) }
    $code = $LASTEXITCODE
} finally {
    $writer.Close()
}
if ($code -ne 0) { exit $code }
""
go run ./summarize $Out
