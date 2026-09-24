param(
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RuntimeRoot = Join-Path $ProjectRoot ".runtime"
$JavaHome = Join-Path $RuntimeRoot "java_home"
$JavaArchive = Join-Path $RuntimeRoot "temurin17.zip"
$HadoopBin = Join-Path $ProjectRoot "hadoop\bin"

$JavaUrl = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1.zip"
$JavaSha256 = "e53a79c3c3d86865bd7e787903884331068e71321714ffd44f145785affc7cb0"
$WinutilsUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
$WinutilsSha256 = "496a591eb1e67df2a620f710d529ba6ddfe1c19149e6647cc4e320bb0efd8553"
$HadoopDllUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll"
$HadoopDllSha256 = "d7ab36a68518748cef142be2da5069b4c763c2cd764c1d2e6ac48c7200405be3"

function Assert-Hash([string]$Path, [string]$Expected) {
    $Actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Actual -ne $Expected) {
        throw "SHA-256 mismatch for $Path`: expected $Expected, got $Actual"
    }
}

New-Item -ItemType Directory -Force -Path $RuntimeRoot, $HadoopBin | Out-Null

if (-not (Test-Path -LiteralPath (Join-Path $JavaHome "bin\java.exe"))) {
    Invoke-WebRequest -Uri $JavaUrl -OutFile $JavaArchive
    Assert-Hash $JavaArchive $JavaSha256
    $ExtractRoot = Join-Path $RuntimeRoot "jdk-extracted"
    if (Test-Path -LiteralPath $ExtractRoot) {
        Remove-Item -LiteralPath $ExtractRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Path $ExtractRoot | Out-Null
    Expand-Archive -LiteralPath $JavaArchive -DestinationPath $ExtractRoot
    $Candidate = Get-ChildItem -LiteralPath $ExtractRoot -Directory | Select-Object -First 1
    if ($null -eq $Candidate -or -not (Test-Path -LiteralPath (Join-Path $Candidate.FullName "bin\java.exe"))) {
        throw "Downloaded archive does not contain a JDK"
    }
    if (Test-Path -LiteralPath $JavaHome) {
        Remove-Item -LiteralPath $JavaHome -Recurse -Force
    }
    Move-Item -LiteralPath $Candidate.FullName -Destination $JavaHome
}

$Winutils = Join-Path $HadoopBin "winutils.exe"
$HadoopDll = Join-Path $HadoopBin "hadoop.dll"
if (-not (Test-Path -LiteralPath $Winutils)) {
    Invoke-WebRequest -Uri $WinutilsUrl -OutFile $Winutils
}
if (-not (Test-Path -LiteralPath $HadoopDll)) {
    Invoke-WebRequest -Uri $HadoopDllUrl -OutFile $HadoopDll
}
Assert-Hash $Winutils $WinutilsSha256
Assert-Hash $HadoopDll $HadoopDllSha256

$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $VenvPython)) {
    & $PythonExe -m venv (Join-Path $ProjectRoot ".venv")
}
& $VenvPython -m pip install -r (Join-Path $ProjectRoot "requirements-lab2.txt")
& $VenvPython (Join-Path $ProjectRoot "scripts\check_environment.py")
