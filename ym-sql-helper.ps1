function SplitSqlPackageScript {
    param (
        [string]$ReportFilePath,
        [string]$SQLFilePath
    )

    [xml]$report = Get-Content $ReportFilePath
    $createTableAsSelects = ($report.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'CreateTableAsSelect' }).Item.Value
    $tableRebuilds = ($report.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'TableRebuild' }).Item.Value

    $script = Get-Content $SQLFilePath

    SplitSql -ScriptContent $script -TableNames $createTableAsSelects -TargetPath .\TableAsSelectSQL
    SplitSql -ScriptContent $script -TableNames $tableRebuilds -TargetPath .\TableRebuildSQL
}

Function SplitSql {
    param (
        $ScriptContent,
        $TableNames,
        $TargetPath
    )

    If (Test-Path $TargetPath) {
        Remove-Item $TargetPath -Force -Recurse
    }
    New-Item $TargetPath -Type directory -Force

    foreach ($table in $TableNames) {
        Write-Host $table 'is be extracted'

        $tableSQL = GetSQLScriptHead($ScriptContent);
        $tableEscape = $table.Replace('[', '\[').Replace(']', '\]').Replace('.', '\.')

        $beginMatch = 0
        for ($i = 41; $i -lt $ScriptContent.Length; $i = $i + 1) {

            if ($beginMatch -gt 0) {
                $tableSQL += $ScriptContent[$i]
            }
            else {

                $hit = $false
                switch ($ScriptContent[$i]) {
                    { $_ -match ('PRINT N''Create Table as Select on ' + $tableEscape + '') } { $hit = $true }
                    { $_ -match ('PRINT N''Creating Primary Key unnamed constraint on ' + $tableEscape + '') } { $hit = $true }
                    { $_ -match ('PRINT N''Starting rebuilding table ' + $tableEscape + '') } { $hit = $true }
                }

                if ($hit) {
                    $tableSQL += $ScriptContent[$i]
                    $beginMatch = 2
                }
            }

            if ( $ScriptContent[$i] -match 'GO') {
                $beginMatch -= 1;
            }
        }

        $tableName = ([regex]::Matches($table , '(?<=\[)(.*?)(?=\])'))[1].Value
        Set-Content -value  $tableSQL -Encoding unicode  -LiteralPath  ($TargetPath + '\' + $tableName + '.sql')
    }
}

Function GetSQLScriptHead {
    param (
        $ScriptContent
    )

    $result = @()

    foreach ($i in 0..40) {
        $result += $ScriptContent[$i]
    }

    return $result
}

Function ParallelExecAllScript {
    param (
        [string]$ConnString,
        [int]$Parallelcount = 4
    )

    ParallelExecSQL -TableSQLFilePath .\TableRebuildSQL\ -ConnString $ConnString -Parallelcount $Parallelcount
    ParallelExecSQL -TableSQLFilePath .\TableAsSelectSQL\ -ConnString $ConnString -Parallelcount $Parallelcount
}

Function ParallelExecSQL {
    param (
        [string]$TableSQLFilePath,
        [string]$ConnString,
        [int]$Parallelcount = 4
    )

    $files = @(Get-ChildItem $TableSQLFilePath)

    if ($files.Length -eq 0) {
        return
    }
    
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
        $completed = $true

        try { 
            Invoke-Sqlcmd -connectionString $connString -Inputfile $file
        }
        catch {
            $completed = $false 
            Write-Warning "$file has an error occurred. $Error"
        }

        if ($completed) {
            Write-Host "$file Completed"
        }
    };

    foreach ($i in 1..$Parallelcount) {

        $job = Start-Job -ScriptBlock $task -Name "task$i" -ArgumentList $files[$i - 1].FullName, $ConnString
    }

    $nextIndex = $Parallelcount
    
    while (($nextIndex -lt $files.Length) -or ($taskCount -gt 0)) {

        $jobs = Get-Job
        foreach ($job in $jobs) {
            $state = [string]$job.State
            if (($state -eq "Completed") -or ($state -eq "Failed")) {
                Receive-Job -Job $job   
                Remove-Job $job
                $taskCount--
                if ($nextIndex -lt $files.Length) {   
                    $taskNumber = $nextIndex + 1
                    $job = Start-Job -ScriptBlock $task -Name "task$taskNumber" -ArgumentList $files[$nextIndex].FullName, $connString
                    $nextIndex++
                }
            }
        }
        Start-Sleep 1
    }
}