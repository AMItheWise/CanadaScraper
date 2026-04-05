param(
  [string]$TaskName = "CanadaHSSportsSync",
  [string]$PythonExe = "python",
  [string]$ConfigPath = "config.yaml",
  [string]$StartTime = "06:00",
  [string]$WorkingDirectory = (Get-Location).Path
)

$command = "cmd /c \"cd /d `"$WorkingDirectory`" && `"$PythonExe`" -m canadastats --config `"$ConfigPath`" sync all\""

schtasks /Create /F /SC DAILY /ST $StartTime /TN $TaskName /TR $command | Out-Host
Write-Host "Scheduled task '$TaskName' created."
