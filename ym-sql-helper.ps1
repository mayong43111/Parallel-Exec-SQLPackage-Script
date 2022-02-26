function SplitSqlPackageScript {
    param (
        [string]$ReportFilePath,
        [string]$SQLFilePath
    )

    [xml]$report = Get-Content $ReportFilePath
    $script = Get-Content $SQLFilePath
    
    SplitSqlForTableRebuild -ReportContent $report -ScriptContent $script
    SplitSqlForCreateTableAsSelect -ReportContent $report -ScriptContent $script
}

Function ParallelExecAllScript {
    param (
        [string]$ConnString,
        [int]$Parallelcount = 4
    )

    ParallelExecSQL -TableSQLFilePath .\TableRebuildSQL\ -ConnString $ConnString -Parallelcount $Parallelcount
    ParallelExecSQL -TableSQLFilePath .\TableAsSelectSQL\ -ConnString $ConnString -Parallelcount $Parallelcount
}

Function SplitSqlForCreateTableAsSelect {
    param (
        $ReportContent,
        $ScriptContent
    )
    $createTableAsSelects = ($ReportContent.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'CreateTableAsSelect' }).Item.Value

    $directory = '.\TableAsSelectSQL'
    If (Test-Path $directory) {
        Remove-Item $directory -Force -Recurse
    }
    New-Item $directory -Type directory -Force

    foreach ($table in $createTableAsSelects) {
        Write-Host $table 'is create table as selects'

        $tableSQL = @()
        $beginMatch = $false;
        $tableEscape = $table.Replace('[', '\[').Replace(']', '\]').Replace('.', '\.')

        for ($i = 1; $i -lt $ScriptContent.Length; $i = $i + 1) {

            if ($beginMatch) {
               
                if ( $ScriptContent[$i] -match 'PRINT') {
                    $beginMatch = $false;
                    break
                }

                $tableSQL += $ScriptContent[$i]
            }

            if (!$beginMatch -and ($ScriptContent[$i] -match ('PRINT N''Create Table as Select on ' + $tableEscape + ''))) {
                $tableSQL += $ScriptContent[$i]
                $beginMatch = $true
            }
        }

        $tableName = ([regex]::Matches($table , '(?<=\[)(.*?)(?=\])'))[1].Value
        Set-Content -value  $tableSQL -Encoding unicode  -LiteralPath  ($directory + '\' + $tableName + '.sql')
    }
}

Function SplitSqlForTableRebuild {
    param (
        $ReportContent,
        $ScriptContent
    )
    $tableRebuilds = ($ReportContent.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'TableRebuild' }).Item.Value

    $directory = '.\TableRebuildSQL'
    If (Test-Path $directory) {
        Remove-Item $directory -Force -Recurse
    }
    New-Item $directory -Type directory -Force

    foreach ($table in $tableRebuilds) {
        Write-Host $table 'is rebuilding table'

        $tableSQL = @()
        $beginMatch = $false;
        $tableEscape = $table.Replace('[', '\[').Replace(']', '\]').Replace('.', '\.')

        for ($i = 1; $i -lt $ScriptContent.Length; $i = $i + 1) {

            if ($beginMatch) {

                if ( $ScriptContent[$i] -match 'PRINT') {
                    $beginMatch = $false;
                    break
                }

                $tableSQL += $ScriptContent[$i]
            }

            if (!$beginMatch -and ($ScriptContent[$i] -match ('PRINT N''Starting rebuilding table ' + $tableEscape + ''))) {
                $tableSQL += $ScriptContent[$i]
                $beginMatch = $true
            }
        }

        $tableName = ([regex]::Matches($table , '(?<=\[)(.*?)(?=\])'))[1].Value
        Set-Content -value  $tableSQL -Encoding unicode  -LiteralPath  ($directory + '\' + $tableName + '.sql')
    }
}
Function ParallelExecSQL {
    param (
        [string]$TableSQLFilePath,
        [string]$ConnString,
        [int]$Parallelcount = 4
    )

    $files = Get-ChildItem $TableSQLFilePath
    
    Remove-Job *

    $taskCount = $files.Length
    if ($Parallelcount -gt $files.Length) {
        $Parallelcount = $files.Length
    }

    $task = { 
        param (
            [string] $file, 
            [string] $connString
        )
        #Write-Host $file
        #Write-Host $connString

        try { 
            Invoke-Sqlcmd -connectionString $connString -Inputfile $file
        }
        catch { 
            Write-Host "An error occurred." -ForegroundColor Red
        }
        Write-Host "$file Completed" -ForegroundColor Yellow
    };

    foreach ($i in 1..$Parallelcount) {

        Start-Job -ScriptBlock $task -Name "task$i" -ArgumentList $files[$i - 1].FullName, $ConnString
    }

    $nextIndex = $Parallelcount
    
    while (($nextIndex -lt $files.Length) -or ($taskCount -gt 0)) {
        foreach ($job in Get-Job) {
            $state = [string]$job.State
            if (($state -eq "Completed") -or ($state -eq "Failed")) {
                Receive-Job -Job $job   
                Remove-Job $job
                $taskCount--
                if ($nextIndex -lt $files.Length) {   
                    $taskNumber = $nextIndex + 1
                    Start-Job -ScriptBlock $task -Name "task$taskNumber" -ArgumentList $files[$nextIndex].FullName, $connString
                    $nextIndex++
                }
            }
        }
        Start-Sleep 1
    }
}