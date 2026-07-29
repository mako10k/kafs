[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
    [string]$Distro = "Ubuntu",
    [string]$StateRoot = "",
    [string]$LinuxRunner = "",
    [ValidateSet("journal_publish", "checkpoint_copy", "metadata_apply", "journal_reclaim")]
    [string[]]$Fault = @(
        "journal_publish",
        "checkpoint_copy",
        "metadata_apply",
        "journal_reclaim"
    ),
    [ValidateRange(10, 600)]
    [int]$MarkerTimeoutSeconds = 120,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

function Invoke-WslChecked {
    param([string[]]$CommandArguments)

    $output = & wsl.exe -d $Distro --exec @CommandArguments
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "WSL command failed with exit code ${exitCode}: $($CommandArguments -join ' ')"
    }
    return $output
}

function Get-DistroVhdx {
    $lxss = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss"
    $record = Get-ChildItem $lxss | ForEach-Object {
        Get-ItemProperty $_.PSPath
    } | Where-Object { $_.DistributionName -eq $Distro } | Select-Object -First 1
    if ($null -eq $record) {
        throw "WSL distro is not registered for this user: $Distro"
    }
    $vhdFileName = "ext4.vhdx"
    if ($record.PSObject.Properties.Name -contains "VhdFileName" -and $record.VhdFileName) {
        $vhdFileName = [string]$record.VhdFileName
    }
    $vhdxPath = Join-Path ([string]$record.BasePath) $vhdFileName
    if ([IO.Path]::GetExtension($vhdxPath) -ne ".vhdx") {
        throw "registered distro storage is not VHDX: $vhdxPath"
    }
    $item = Get-Item -LiteralPath $vhdxPath
    return [PSCustomObject]@{
        Distro = $Distro
        Path = $item.FullName
        Length = [Int64]$item.Length
        LastWriteTimeUtc = $item.LastWriteTimeUtc.ToString("o")
    }
}

function Resolve-LinuxRunner {
    if ($LinuxRunner) {
        return $LinuxRunner
    }
    $windowsRunner = Join-Path $PSScriptRoot "v7-vhdx-host-recovery.sh"
    $resolved = Invoke-WslChecked @("/usr/bin/wslpath", "-a", "-u", $windowsRunner)
    return ([string]$resolved).Trim()
}

function Resolve-StateRoot {
    if ($StateRoot) {
        return $StateRoot
    }
    $wslHome = Invoke-WslChecked @("/usr/bin/printenv", "HOME")
    return "$(([string]$wslHome).Trim())/.local/state/kafs-v7-vhdx-recovery"
}

function Receive-ArmJobBounded {
    param([System.Management.Automation.Job]$Job)

    $completed = Wait-Job -Job $Job -Timeout 30
    if ($null -eq $completed) {
        Stop-Job -Job $Job
    }
    $output = Receive-Job -Job $Job 2>&1 | Out-String
    Remove-Job -Job $Job -Force
    return $output
}

$vhdx = Get-DistroVhdx
$LinuxRunner = Resolve-LinuxRunner
$StateRoot = Resolve-StateRoot
Invoke-WslChecked @($LinuxRunner, "--preflight", "--state-root", $StateRoot) |
    ForEach-Object { Write-Host $_ }

$preflight = [ordered]@{
    schema = "KAFS.V7VhdxHostPreflight.v1"
    distro = $Distro
    vhdx_path = $vhdx.Path
    vhdx_length = $vhdx.Length
    vhdx_last_write_utc = $vhdx.LastWriteTimeUtc
    linux_runner = $LinuxRunner
    state_root = $StateRoot
    execute_requested = [bool]$Execute
    raw_vhdx_access = $false
    real_media_qualified = $false
    physical_power_interruption = $false
    controller_independent_wear_qualified = $false
    release_candidate_qualified = $false
}
Write-Output ([PSCustomObject]$preflight)

if (-not $Execute) {
    Write-Host "KAFS_V7_VHDX_HOST_PREFLIGHT PASS (no distro termination requested)"
    return
}

$target = "WSL distro '$Distro' backed by '$($vhdx.Path)'"
if (-not $PSCmdlet.ShouldProcess($target, "terminate and restart at each durable KAFS fault marker")) {
    return
}

$runId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ") + "-" +
    ([Guid]::NewGuid().ToString("N").Substring(0, 8))
$results = @()

foreach ($faultName in $Fault) {
    $stateDir = "$StateRoot/$runId/$faultName"
    $marker = "$stateDir/pause.marker"
    $armArguments = @(
        "-d", $Distro, "--exec", $LinuxRunner,
        "--arm", "--fault", $faultName,
        "--state-root", $StateRoot, "--state-dir", $stateDir
    )
    $armJob = Start-Job -ScriptBlock {
        param([string[]]$Arguments)
        & wsl.exe @Arguments
        return $LASTEXITCODE
    } -ArgumentList (, $armArguments)

    $deadline = [DateTime]::UtcNow.AddSeconds($MarkerTimeoutSeconds)
    $markerSeen = $false
    while ([DateTime]::UtcNow -lt $deadline) {
        & wsl.exe -d $Distro --exec /usr/bin/test -f $marker
        if ($LASTEXITCODE -eq 0) {
            $markerSeen = $true
            break
        }
        if ($armJob.State -eq "Failed" -or $armJob.State -eq "Completed") {
            break
        }
        Start-Sleep -Milliseconds 250
    }
    if (-not $markerSeen) {
        & wsl.exe --terminate $Distro
        $cleanupExit = $LASTEXITCODE
        $armOutput = Receive-ArmJobBounded $armJob
        throw "durable marker timeout for $faultName; cleanup terminate exit=$cleanupExit; arm=$armOutput"
    }
    if ($armJob.State -ne "Running") {
        & wsl.exe --terminate $Distro
        $cleanupExit = $LASTEXITCODE
        $armOutput = Receive-ArmJobBounded $armJob
        throw "arm process was not active at marker for $faultName; cleanup terminate exit=$cleanupExit; arm=$armOutput"
    }

    $markerValue = Invoke-WslChecked @("/usr/bin/cat", $marker)
    if (([string]$markerValue).Trim() -ne $faultName) {
        & wsl.exe --terminate $Distro
        $cleanupExit = $LASTEXITCODE
        Receive-ArmJobBounded $armJob | Out-Null
        throw "durable marker mismatch for $faultName; cleanup terminate exit=$cleanupExit"
    }

    $markerObservedUtc = [DateTime]::UtcNow.ToString("o")
    $lengthBefore = (Get-Item -LiteralPath $vhdx.Path).Length
    & wsl.exe --terminate $Distro
    $terminateExit = $LASTEXITCODE
    $terminatedUtc = [DateTime]::UtcNow.ToString("o")
    $armOutput = Receive-ArmJobBounded $armJob
    if ($terminateExit -ne 0) {
        throw "wsl --terminate failed for $faultName with exit code $terminateExit; arm=$armOutput"
    }

    & wsl.exe -d $Distro --exec /usr/bin/true
    $restartExit = $LASTEXITCODE
    $restartedUtc = [DateTime]::UtcNow.ToString("o")
    if ($restartExit -ne 0) {
        throw "WSL restart failed for $faultName with exit code $restartExit"
    }
    $lengthAfter = (Get-Item -LiteralPath $vhdx.Path).Length

    $hostRecord = [ordered]@{
        schema = "KAFS.V7VhdxHostController.v1"
        fault = $faultName
        distro = $Distro
        vhdx_path = $vhdx.Path
        vhdx_length_before = [Int64]$lengthBefore
        vhdx_length_after = [Int64]$lengthAfter
        marker_observed_utc = $markerObservedUtc
        terminated_utc = $terminatedUtc
        restarted_utc = $restartedUtc
        terminate_exit_code = [int]$terminateExit
        restart_exit_code = [int]$restartExit
        controller = "wsl.exe --terminate"
        raw_vhdx_access = $false
        real_media_qualified = $false
        physical_power_interruption = $false
        controller_independent_wear_qualified = $false
        release_candidate_qualified = $false
    }
    $temporaryHostRecord = [IO.Path]::GetTempFileName()
    try {
        $json = ([PSCustomObject]$hostRecord | ConvertTo-Json -Depth 4)
        [IO.File]::WriteAllText(
            $temporaryHostRecord,
            $json + [Environment]::NewLine,
            (New-Object Text.UTF8Encoding($false))
        )
        $hostEvidenceWsl = Invoke-WslChecked @(
            "/usr/bin/wslpath", "-a", "-u", $temporaryHostRecord
        )
        Invoke-WslChecked @(
            $LinuxRunner, "--verify", "--fault", $faultName,
            "--state-root", $StateRoot, "--state-dir", $stateDir,
            "--host-evidence", ([string]$hostEvidenceWsl).Trim()
        ) | ForEach-Object { Write-Host $_ }
    }
    finally {
        Remove-Item -LiteralPath $temporaryHostRecord -Force -ErrorAction SilentlyContinue
    }

    $results += [PSCustomObject]@{
        fault = $faultName
        state_dir = $stateDir
        terminate_exit_code = $terminateExit
        restart_exit_code = $restartExit
        verification = "PASS"
    }
}

Write-Host "KAFS_V7_VHDX_HOST_RECOVERY PASS run_id=$runId"
Write-Output $results
