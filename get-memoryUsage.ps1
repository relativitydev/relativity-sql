#this is not a SQL query, it is a powershell. 
# see https://www.petri.com/display-memory-usage-powershell  for oirginal source.  This script was not written by anyone at Relativity.  That web page chops the code up into pieces, this combines them for your convenience. 
Function Test-MemoryUsage {
[cmdletbinding()]
Param()
 
$os = Get-Ciminstance Win32_OperatingSystem
$pctFree = [math]::Round(($os.FreePhysicalMemory/$os.TotalVisibleMemorySize)*100,2)
 
if ($pctFree -ge 45) {
$Status = "OK"
}
elseif ($pctFree -ge 15 ) {
$Status = "Warning"
}
else {
$Status = "Critical"
}
 
$os | Select @{Name = "Status";Expression = {$Status}},
@{Name = "% Free"; Expression = {$pctFree}},
@{Name = "Free Gigs";Expression = {[math]::Round($_.FreePhysicalMemory/1mb,2)}},
@{Name = "Total Gigs";Expression = {[int]($_.TotalVisibleMemorySize/1mb)}}
 
}

Function Show-MemoryUsage {
 
[cmdletbinding()]
Param()
 
#get memory usage data
$data = Test-MemoryUsage
 
Switch ($data.Status) {
"OK" { $color = "cyan" }
"Warning" { $color = "Yellow" }
"Critical" {$color = "Red" }
}
 
$title = @"
 
Memory Check
------------
"@
 
Write-Host $title -foregroundColor Green
 
$data | Format-Table -AutoSize | Out-String | Write-Host -ForegroundColor $color
 
}
Show-MemoryUsage
 
set-alias -Name smu -Value Show-MemoryUsage
