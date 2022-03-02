function InvokeSqlcmd {
    param (
        [string]$ConnectionString,
        [string]$Query,
        [string]$Inputfile
    )

    if ([string]::IsNullOrWhiteSpace($ConnectionString)) {

        Write-Warning "ConnectionString not exist"
        return
    }

    $sb = New-Object System.Data.Common.DbConnectionStringBuilder
    $sb.set_ConnectionString($ConnectionString)

    if (($sb['authentication'] -eq 'Active Directory Password') -or ($sb['authentication'] -eq 'ActiveDirectoryPassword')) {
        
        $userName = $sb['User ID']
        $password = $sb['Password']
        $server = $sb['Data Source']
        $database = $sb['Initial Catalog']

        
        if (-not [string]::IsNullOrWhiteSpace($Inputfile)) {
            Sqlcmd -I -S $server -d $database -U $userName -P $password -G -i $Inputfile
        }
        elseif (-not [string]::IsNullOrWhiteSpace($Query)) {
            Sqlcmd -I -S $server -d $database -U $userName -P $password -G -Q $Query
        }
        else {
            Write-Error 'No match Sqlcmd param'
        }
    }
    else {

        ImportSqlServer

        if (-not [string]::IsNullOrWhiteSpace($Inputfile)) {
            Invoke-Sqlcmd -connectionString $connString -Inputfile $Inputfile 
        }
        elseif (-not [string]::IsNullOrWhiteSpace($Query)) {
            Invoke-Sqlcmd -connectionString $connString -Query $Query
        }
        else {
            Write-Error 'No match Sqlcmd param'
        }
    }
}

function ImportSqlServer {

    if (!(Get-Module -ListAvailable -Name SqlServer)) {
        Write-Host "SqlServer Module does not exist"
        Install-Module -Name SqlServer -Force
    }
}