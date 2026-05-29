$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

$python = Get-Command python -ErrorAction Stop
$runRoot = Join-Path $projectRoot "run"
$sourceFile = Join-Path $runRoot "arquivo_original.bin"
$peerADir = Join-Path $runRoot "peer_A"
$peerBDir = Join-Path $runRoot "peer_B"
$peerAStdout = Join-Path $runRoot "peer_A_stdout.log"
$peerAStderr = Join-Path $runRoot "peer_A_stderr.log"
$peerBStdout = Join-Path $runRoot "peer_B_stdout.log"
$peerBStderr = Join-Path $runRoot "peer_B_stderr.log"
$blockSize = 1024
$peerA = $null
$peerB = $null

function Resolve-AbsolutePath {
    param([Parameter(Mandatory = $true)][string]$Path)
    return [System.IO.Path]::GetFullPath($Path)
}

function Remove-DirectorySafely {
    param(
        [Parameter(Mandatory = $true)][string]$TargetPath,
        [Parameter(Mandatory = $true)][string]$AllowedRoot
    )

    $resolvedTarget = Resolve-AbsolutePath $TargetPath
    $resolvedRoot = Resolve-AbsolutePath $AllowedRoot
    if (-not $resolvedTarget.StartsWith($resolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Recusando remover caminho fora de ${resolvedRoot}: $resolvedTarget"
    }

    if (Test-Path -LiteralPath $resolvedTarget) {
        Remove-Item -LiteralPath $resolvedTarget -Recurse -Force
    }
}

function Initialize-SourceFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (Test-Path -LiteralPath $Path) {
        return
    }

    $createCommand = @(
        "-c",
        "from pathlib import Path; p = Path(r'$Path'); p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(bytes(range(256)) * 256)"
    )
    & $python.Source @createCommand
    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao criar arquivo de teste em $Path"
    }
}

function Start-PeerProcess {
    param(
        [Parameter(Mandatory = $true)][string]$StdoutPath,
        [Parameter(Mandatory = $true)][string]$StderrPath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    if (Test-Path -LiteralPath $StdoutPath) {
        Remove-Item -LiteralPath $StdoutPath -Force
    }
    if (Test-Path -LiteralPath $StderrPath) {
        Remove-Item -LiteralPath $StderrPath -Force
    }

    return Start-Process `
        -FilePath $python.Source `
        -ArgumentList $Arguments `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -WindowStyle Hidden `
        -PassThru
}

function Flush-LogLines {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Offsets
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $lines = @(Get-Content -LiteralPath $Path)
    $start = 0
    if ($Offsets.ContainsKey($Path)) {
        $start = [int]$Offsets[$Path]
    }

    for ($i = $start; $i -lt $lines.Count; $i++) {
        Write-Host "[$Label] $($lines[$i])"
    }

    $Offsets[$Path] = $lines.Count
}

function Stop-ProcessQuietly {
    param([System.Diagnostics.Process]$Process)

    if ($null -eq $Process) {
        return
    }

    try {
        if (-not $Process.HasExited) {
            $Process.Kill()
            $Process.WaitForExit()
        }
    } catch {
    }
}

try {
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    Initialize-SourceFile -Path $sourceFile

    Remove-DirectorySafely -TargetPath $peerADir -AllowedRoot $runRoot
    Remove-DirectorySafely -TargetPath $peerBDir -AllowedRoot $runRoot
    New-Item -ItemType Directory -Path $peerADir -Force | Out-Null
    New-Item -ItemType Directory -Path $peerBDir -Force | Out-Null

    $seederArgs = @(
        "-m", "p2p_transfer.peer",
        "--peer-id", "A",
        "--host", "127.0.0.1",
        "--port", "5001",
        "--neighbors", "127.0.0.1:5002",
        "--data-dir", ".\run\peer_A",
        "--file", ".\run\arquivo_original.bin",
        "--target", "arquivo_original.bin",
        "--block-size", "$blockSize",
        "--serve-only",
        "--max-runtime", "120"
    )

    $leecherArgs = @(
        "-m", "p2p_transfer.peer",
        "--peer-id", "B",
        "--host", "127.0.0.1",
        "--port", "5002",
        "--neighbors", "127.0.0.1:5001",
        "--data-dir", ".\run\peer_B",
        "--target", "arquivo_original.bin",
        "--block-size", "$blockSize",
        "--exit-when-complete"
    )

    Write-Host "Iniciando peer A e peer B..."
    $peerA = Start-PeerProcess -StdoutPath $peerAStdout -StderrPath $peerAStderr -Arguments $seederArgs
    Start-Sleep -Milliseconds 600
    $peerB = Start-PeerProcess -StdoutPath $peerBStdout -StderrPath $peerBStderr -Arguments $leecherArgs

    $offsets = @{}
    while (-not $peerB.HasExited) {
        Flush-LogLines -Label "A" -Path $peerAStdout -Offsets $offsets
        Flush-LogLines -Label "A" -Path $peerAStderr -Offsets $offsets
        Flush-LogLines -Label "B" -Path $peerBStdout -Offsets $offsets
        Flush-LogLines -Label "B" -Path $peerBStderr -Offsets $offsets
        Start-Sleep -Milliseconds 150
        $peerA.Refresh()
        $peerB.Refresh()
    }

    $peerB.WaitForExit()
    $peerB.Refresh()

    Flush-LogLines -Label "A" -Path $peerAStdout -Offsets $offsets
    Flush-LogLines -Label "A" -Path $peerAStderr -Offsets $offsets
    Flush-LogLines -Label "B" -Path $peerBStdout -Offsets $offsets
    Flush-LogLines -Label "B" -Path $peerBStderr -Offsets $offsets

    if (-not $peerA.HasExited) {
        $peerA.Kill()
        $peerA.WaitForExit()
    }

    Flush-LogLines -Label "A" -Path $peerAStdout -Offsets $offsets
    Flush-LogLines -Label "A" -Path $peerAStderr -Offsets $offsets

    $leecherExitCode = [int]$peerB.ExitCode
    if ($leecherExitCode -ne 0) {
        throw "O leecher encerrou com codigo $leecherExitCode. Veja $peerBStdout."
    }

    $downloadedFile = Join-Path $peerBDir "downloads\arquivo_original.bin"
    if (-not (Test-Path -LiteralPath $downloadedFile)) {
        throw "O arquivo final nao foi encontrado em $downloadedFile"
    }

    Write-Host ""
    Write-Host "Transferencia concluida com sucesso."
    Write-Host "Arquivo baixado: $downloadedFile"
    Write-Host "Logs completos:"
    Write-Host "  $peerAStdout"
    Write-Host "  $peerAStderr"
    Write-Host "  $peerBStdout"
    Write-Host "  $peerBStderr"
} finally {
    Stop-ProcessQuietly -Process $peerA
    Stop-ProcessQuietly -Process $peerB
}
