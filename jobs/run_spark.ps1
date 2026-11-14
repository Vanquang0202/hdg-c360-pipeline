# Mục đích: Chạy một job PySpark bằng Python 3.11 và classpath JDBC local.
# Đầu vào: -Job full_load/reconcile/check_jdbc, đường dẫn Python/JAR và JobArguments.
# Đầu ra: Tiến trình Spark local[2] cùng mã thoát của job; không tự chạy DDL.
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("full_load", "reconcile", "check_jdbc")]
    [string]$Job,

    [string]$Python = "C:\Python311\python.exe",
    [string]$OracleJar = (Join-Path $PSScriptRoot "jars\ojdbc11.jar"),
    [string]$PostgresJar = (Join-Path $PSScriptRoot "jars\postgresql-42.7.14.jar"),
    [string[]]$JobArguments = @()
)

$ErrorActionPreference = "Stop"
# BƯỚC 1: Kiểm file và phiên bản PySpark trước khi khởi động JVM.
foreach ($file in @($Python, $OracleJar, $PostgresJar)) {
    if (-not (Test-Path -LiteralPath $file -PathType Leaf)) {
        throw "Required file is missing: $file. See jobs/FULL_LOAD.md."
    }
}

$Python = (Resolve-Path -LiteralPath $Python).Path
$OracleJar = (Resolve-Path -LiteralPath $OracleJar).Path
$PostgresJar = (Resolve-Path -LiteralPath $PostgresJar).Path

$sparkVersion = & $Python -c "import pyspark; print(pyspark.__version__)"
if ($LASTEXITCODE -ne 0 -or $sparkVersion.Trim() -ne "4.0.1") {
    throw "Install jobs/requirements-spark.txt in the selected Python environment."
}
$sparkHome = & $Python -c "from pathlib import Path; import pyspark; print(Path(pyspark.__file__).parent)"
if ($LASTEXITCODE -ne 0) { throw "Cannot locate the PySpark distribution." }

# BƯỚC 2: Cố định Python/Spark cho driver và worker để không dùng nhầm venv cũ.
$env:SPARK_HOME = $sparkHome.Trim()
$env:PYSPARK_PYTHON = $Python
$env:PYSPARK_DRIVER_PYTHON = $Python

$submit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
# BƯỚC 3: Hai JAR cùng ở máy local; classpath tránh phân phối file thừa qua Hadoop fetchFile.
$classPath = @($OracleJar, $PostgresJar) -join [IO.Path]::PathSeparator
$sparkArgs = @(
    "--master", "local[2]",
    "--driver-memory", "1g",
    "--driver-class-path", $classPath,
    "--conf", "spark.executor.extraClassPath=$classPath",
    "--conf", "spark.jars=",
    "--conf", "spark.driver.host=127.0.0.1",
    "--conf", "spark.driver.bindAddress=127.0.0.1",
    "--conf", "spark.sql.shuffle.partitions=2",
    "--conf", "spark.task.maxFailures=1",
    "--conf", "spark.speculation=false"
)
# BƯỚC 4: Chuyển nguyên tham số cho job đã chọn; full_load.py mới quyết định Stage/publish.
# Kiểm JDBC nằm trong tests; hai job xử lý dữ liệu vẫn nằm trong jobs.
$jobFile = if ($Job -eq "check_jdbc") { Join-Path $PSScriptRoot "..\tests\full_load\check_jdbc.py" } else { Join-Path $PSScriptRoot "$Job.py" }
& $submit @sparkArgs $jobFile @JobArguments
exit $LASTEXITCODE
