param(
  [string]$TaskName = "CanadaHSSportsSync"
)

schtasks /Delete /F /TN $TaskName | Out-Host
Write-Host "Scheduled task '$TaskName' removed."
