<#
.SYNOPSIS
    Builds a self-contained, single-file release executable for Copilot Usage.

.DESCRIPTION
    Produces a standalone CopilotUsage.exe that bundles the .NET 10 runtime and all
    WPF native libraries. The target machine does NOT need .NET installed, and no
    additional DLLs are required alongside the executable.

    Output: bin\Publish\win-x64\CopilotUsage.exe

.NOTES
    Trimming is intentionally disabled — WPF relies on reflection and trimming breaks it.
    IncludeNativeLibrariesForSelfExtract is set in the .csproj to bundle WPF native DLLs.
#>

$outputDir = "bin\Publish\win-x64"

Write-Host "Building self-contained release..." -ForegroundColor Cyan

dotnet publish `
    -c Release `
    -r win-x64 `
    --self-contained true `
    -p:PublishSingleFile=true `
    -p:PublishReadyToRun=true `
    -o $outputDir

if ( $LASTEXITCODE -ne 0 )
{
    Write-Error "Build failed (exit code $LASTEXITCODE)."
    exit $LASTEXITCODE
}

$exe = Join-Path $PSScriptRoot "$outputDir\CopilotUsage.exe"
$size = ( Get-Item $exe ).Length / 1MB

Write-Host ""
Write-Host "Build succeeded." -ForegroundColor Green
Write-Host "Output : $exe"
Write-Host ( "Size   : {0:N1} MB" -f $size )
Write-Host ""
Write-Host "Upload '$exe' as a release asset on GitHub:" -ForegroundColor Yellow
Write-Host "  https://github.com/<org>/<repo>/releases/new"
