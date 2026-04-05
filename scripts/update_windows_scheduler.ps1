param(
  [string]$TaskName = "CanadaHSSportsSync",
  [string]$PythonExe = "python",
  [string]$ConfigPath = "config.yaml",
  [string]$StartTime = "06:00",
  [string]$WorkingDirectory = (Get-Location).Path
)

& "$PSScriptRoot\remove_windows_scheduler.ps1" -TaskName $TaskName
& "$PSScriptRoot\setup_windows_scheduler.ps1" -TaskName $TaskName -PythonExe $PythonExe -ConfigPath $ConfigPath -StartTime $StartTime -WorkingDirectory $WorkingDirectory
