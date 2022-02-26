Import-Module .\ym-sql-helper.ps1 -Force 

$reportFilePath = '.\deploy-report.xml'
$sqlFilePath = '.\deploy.sql'

SplitSqlPackageScript -ReportFilePath $reportFilePath -SqlFilePath $sqlFilePath
ParallelExecAllScript -ConnString 'asdasdasd' -Parallelcount 4
