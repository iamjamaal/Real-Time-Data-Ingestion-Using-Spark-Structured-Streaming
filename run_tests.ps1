Param(
    [ValidateSet('unit','integration','all')]
    [string]$Scope = 'unit',
    [switch]$UseDocker
)

$ErrorActionPreference = 'Stop'

function Invoke-Pytest($path) {
    if ($UseDocker) {
        docker exec -w /opt -e PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/src" ecommerce-spark-master python3 -m pytest $path -q
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } else {
        python -m pytest $path -q
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
}

if ($Scope -eq 'unit') {
    $path = 'tests/unit'
    if ($UseDocker) { $path = '/opt/tests/unit' }
    Invoke-Pytest $path
} elseif ($Scope -eq 'integration') {
    $path = 'tests/integration'
    if ($UseDocker) { $path = '/opt/tests/integration' }
    Invoke-Pytest $path
} else {
    $pathUnit = 'tests/unit'
    $pathInt = 'tests/integration'
    if ($UseDocker) {
        $pathUnit = '/opt/tests/unit'
        $pathInt = '/opt/tests/integration'
    }
    Invoke-Pytest $pathUnit
    Invoke-Pytest $pathInt
}
