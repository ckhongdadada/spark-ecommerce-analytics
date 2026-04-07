param(
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$mavenHome = Join-Path $projectRoot ".build-env\apache-maven-3.9.9"
$mavenRepo = Join-Path $projectRoot ".build-env\m2-repo"
$mvn = Join-Path $mavenHome "bin\mvn.cmd"

if (-not (Test-Path -LiteralPath $mvn)) {
    throw "Local Maven was not found. Initialize .build-env first."
}

New-Item -ItemType Directory -Force -Path $mavenRepo | Out-Null
$env:MAVEN_OPTS = "-Dfile.encoding=UTF-8"

$args = @("-Dmaven.repo.local=$mavenRepo", "clean", "package")
if ($SkipTests) {
    $args += "-DskipTests"
}

& $mvn @args
