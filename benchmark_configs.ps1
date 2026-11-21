param(
    [Parameter(Mandatory=$true)][string]$Image,
    [Parameter(Mandatory=$true)][string]$DataDir,
    [Parameter(Mandatory=$true)][string]$OutputRoot,
    [int]$Runs = 5,
    [int]$CpuCount = 8
)

$ErrorActionPreference = 'Stop'
if ($Runs -lt 1) { throw 'Runs must be >= 1' }
$DataDir = (Resolve-Path $DataDir).Path
New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$OutputRoot = (Resolve-Path $OutputRoot).Path

# Keep all settings identical except the one(s) being tested.
$cases = @(
    @{ Label='original-group-count'; Group=4; Mmap=1; Prefault=1 },
    @{ Label='codex-group-count';    Group=2; Mmap=1; Prefault=1 },
    @{ Label='one-group-thread';    Group=1; Mmap=1; Prefault=1 },
    @{ Label='lazy-input';          Group=4; Mmap=0; Prefault=1 },
    @{ Label='lazy-arena';          Group=4; Mmap=1; Prefault=0 },
    @{ Label='both-lazy';          Group=4; Mmap=0; Prefault=0 }
)
$results = @()
foreach ($case in $cases) {
    $seconds = @()
    for ($i=1; $i -le $Runs; $i++) {
        $dir = Join-Path $OutputRoot "$($case.Label)-$i"
        if (Test-Path $dir) { Remove-Item -LiteralPath $dir -Recurse -Force }
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "Running $($case.Label), $i/$Runs"
        $args = @(
            'run','--rm','--network','none',
            '--cpus',"$CpuCount",'--memory','24g',
            '-e',"NCPU=$CpuCount", '-e',"GROUP_MULTIPLIER=$($case.Group)",
            '-e',"MMAP_POPULATE=$($case.Mmap)", '-e',"PREFAULT_WRITE=$($case.Prefault)",
            '--mount',"type=bind,source=$DataDir,target=/data,readonly",
            '--mount',"type=bind,source=$dir,target=/output",
            $Image
        )
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        & docker @args | Out-Null
        $exit = $LASTEXITCODE
        $sw.Stop()
        if ($exit -ne 0) { throw "Docker run failed: case=$($case.Label), exit=$exit" }
        $seconds += [double]$sw.Elapsed.TotalSeconds
    }
    $sorted = @($seconds | Sort-Object)
    if ($Runs % 2 -eq 1) {
        $median = $sorted[[int][Math]::Floor($Runs / 2)]
    } else {
        $median = ($sorted[$Runs / 2 - 1] + $sorted[$Runs / 2]) / 2
    }
    $results += [pscustomobject]@{
        Case = $case.Label
        MedianSeconds = [Math]::Round($median, 4)
        AllSeconds = ($seconds | ForEach-Object { [Math]::Round($_,4) }) -join ','
    }
}
$results | Sort-Object MedianSeconds | Format-Table -AutoSize
$results | Export-Csv -NoTypeInformation -Path (Join-Path $OutputRoot 'benchmark_results.csv')
Write-Host "Report: $(Join-Path $OutputRoot 'benchmark_results.csv')"
