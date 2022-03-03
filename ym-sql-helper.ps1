function ImportSqlServer {

    if (!(Get-Module -ListAvailable -Name SqlServer)) {
        Write-Host "SqlServer Module does not exist"
        Install-Module -Name SqlServer -Force
    }
}

function SplitSqlPackageScript {
    param (
        [string]$ReportFilePath,
        [string]$SQLFilePath
    )

    [xml]$report = Get-Content $ReportFilePath
    $createTableAsSelects = ($report.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'CreateTableAsSelect' }).Item.Value
    $tableRebuilds = ($report.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'TableRebuild' }).Item.Value
    $alterTables = (($report.DeploymentReport.Operations.Operation | Where-Object { $_.name -eq 'Alter' }).Item | Where-Object { $_.Type -eq 'SqlTable' }).Value

    $script = Get-Content $SQLFilePath

    SplitSql -ScriptContent $script -TableNames $createTableAsSelects -TargetPath .\TableAsSelectSQL
    SplitSql -ScriptContent $script -TableNames $tableRebuilds -TargetPath .\TableRebuildSQL
    SplitSql -ScriptContent $script -TableNames $alterTables -TargetPath .\AlterTablesSQL

    SaveOtherSQL -ScriptContent $script -TargetPath .\OtherSQL
}

function SaveOtherSQL {
    param (
        $ScriptContent,
        $TargetPath
    )

    $sql = @()
    $continuous = $false;

    for ($i = 0; $i -lt $ScriptContent.Length; $i = $i + 1) {

        if ($ScriptContent[$i] -match '^\s*$') {

            if ($continuous) {
                continue
            }

            $continuous = $true
        }
        else {
            $continuous = $false
        }

        $sql += $ScriptContent[$i]
    }

    If (Test-Path $TargetPath) {
        Remove-Item $TargetPath -Force -Recurse
    }
    New-Item $TargetPath -Type directory -Force
    Set-Content -value $sql -Encoding unicode  -LiteralPath  ($TargetPath + '\other.sql')
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

                if ($ScriptContent[$i] -match '^GO\s*$') {
                    $beginMatch -= 1;
                }

                $ScriptContent[$i] = ''
            }
            else {

                switch ($ScriptContent[$i]) {
                    { $_ -eq '' } {  }

                    { $_ -match ('^PRINT N''Create Table as Select on ' + $tableEscape + '') } { $beginMatch = 2 }
                    { $_ -match ('^PRINT N''Creating Primary Key unnamed constraint on ' + $tableEscape + '') } { $beginMatch = 2 }
                    
                    { $_ -match ('^PRINT N''Starting rebuilding table ' + $tableEscape + '') } { $beginMatch = 2 }

                    { $_ -match ('^PRINT N''Altering Table ' + $tableEscape + '') } { $beginMatch = 2 }
                    { $_ -match ('^PRINT N''Creating Column Store Index ' + $tableEscape + '') } { $beginMatch = 2 }
                }

                if ($beginMatch -gt 0) {
                    $tableSQL += $ScriptContent[$i]
                    $ScriptContent[$i] = ''
                    $beginMatch = 2
                }
            }
        }

        $fileName = $table.Replace(']', '').Replace('[', '').Replace('.', '_').Replace('\', '_').Replace('/', '_')
        Set-Content -value  $tableSQL -Encoding unicode  -LiteralPath  ($TargetPath + '\' + $fileName + '.sql')
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
    ParallelExecSQL -TableSQLFilePath .\AlterTablesSQL -ConnString $ConnString -Parallelcount $Parallelcount
    ParallelExecSQL -TableSQLFilePath .\OtherSQL\ -ConnString $ConnString -Parallelcount $Parallelcount
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

        $start = Get-Date
        try {
            Import-Module .\InvokeSqlcmd.ps1 -Force

            InvokeSqlcmd -connectionString $connString -Inputfile $file
            #Invoke-Sqlcmd -connectionString $connString -Inputfile $file
        }
        catch {
            $completed = $false 
        }

        $end = Get-Date
        $total = $end - $start

        if ($completed) {
            Write-Host "[$total] $file Completed"
        }
        else {
            Write-Error "[$total] $file has an error occurred. $Error"
        }
    }

    foreach ($i in 1..$Parallelcount) {

        $job = Start-Job -WorkingDirectory $PSScriptRoot -ScriptBlock $task -Name "task$i" -ArgumentList $files[$i - 1].FullName, $ConnString
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
                    $job = Start-Job -WorkingDirectory $PSScriptRoot -ScriptBlock $task -Name "task$taskNumber" -ArgumentList $files[$nextIndex].FullName, $connString
                    $nextIndex++
                }
            }
        }
        Start-Sleep 1
    }
}