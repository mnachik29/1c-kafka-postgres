<#
.SYNOPSIS
  Операции с тестовой информационной базой 1С (Windows, файловый вариант).

.EXAMPLE
  .\scripts\1c.ps1 create          # создать базу 1c\ib, загрузить конфигурацию из 1c\src, настроить OData и заполнить данные
  .\scripts\1c.ps1 publish         # опубликовать базу на Apache 2.4 (нужны права администратора и модули веб-сервера 1С)
  .\scripts\1c.ps1 demo-update     # изменить контрагента ООО «Ромашка» (КПП)
  .\scripts\1c.ps1 demo-delete     # пометить на удаление ООО «Лютик»
  .\scripts\1c.ps1 demo-restore    # снять пометку удаления
  .\scripts\1c.ps1 odata | seed | init
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('create', 'publish', 'init', 'odata', 'seed', 'demo-update', 'demo-delete', 'demo-restore')]
    [string]$Command,
    [string]$Platform = $env:ONEC_PLATFORM_BIN,
    [string]$ApacheConf = 'C:\Apache24\conf\httpd.conf',
    [string]$ApacheHtdocs = 'C:\Apache24\htdocs'
)
$ErrorActionPreference = 'Stop'

$root = Split-Path $PSScriptRoot -Parent
$ib = Join-Path $root '1c\ib'
$src = Join-Path $root '1c\src'
if (-not $Platform) {
    # самая новая установленная платформа (x64 или x86); для publish — только с webinst.exe
    $Platform = Get-ChildItem 'C:\Program Files\1cv8', 'C:\Program Files (x86)\1cv8' -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '^\d+\.\d+\.\d+\.\d+$' } |
        Where-Object { $Command -ne 'publish' -or (Test-Path (Join-Path $_.FullName 'bin\webinst.exe')) } |
        Sort-Object { [version]$_.Name } -Descending |
        Select-Object -First 1 | ForEach-Object { Join-Path $_.FullName 'bin' }
}
$v8 = Join-Path $Platform '1cv8.exe'
$log = Join-Path $env:TEMP '1c-integration.log'

function Invoke-1C([string]$Arguments) {
    Write-Host "1cv8 $Arguments"
    $p = Start-Process $v8 -ArgumentList "$Arguments /Out `"$log`" /DisableStartupDialogs /DisableStartupMessages" -Wait -PassThru
    if (Test-Path $log) { Get-Content $log -Encoding Default | Write-Host }
    if ($p.ExitCode -ne 0) { throw "1cv8 exited with code $($p.ExitCode)" }
}

switch ($Command) {
    'create' {
        if (Test-Path $ib) { throw "Infobase already exists: $ib (delete it to recreate)" }
        Invoke-1C "CREATEINFOBASE File=`"$ib`""
        Invoke-1C "DESIGNER /F `"$ib`" /LoadConfigFromFiles `"$src`" /UpdateDBCfg"
        Invoke-1C "ENTERPRISE /F `"$ib`" /C init"
    }
    'publish' {
        $webinst = Join-Path $Platform 'webinst.exe'
        if (-not (Test-Path $webinst)) {
            throw "webinst.exe not found: install the '1C:Enterprise web server extension modules' component (see 1c/setup.md)"
        }
        $dir = Join-Path $ApacheHtdocs 'demo1c'
        & $webinst -publish -apache24 -wsdir demo1c -dir $dir -connstr "File=`"$ib`";" -confpath $ApacheConf
        Copy-Item (Join-Path $root '1c\default.vrd') (Join-Path $dir 'default.vrd') -Force
        (Get-Content (Join-Path $dir 'default.vrd') -Raw).Replace('__IB_PATH__', $ib) |
            Set-Content (Join-Path $dir 'default.vrd') -Encoding UTF8
        Write-Host 'Published. Restart Apache: httpd -k restart (or restart the Apache2.4 service)'
    }
    default {
        Invoke-1C "ENTERPRISE /F `"$ib`" /C $Command"
    }
}
