<#
  WaterManagement - script de despliegue del LABORATORIO (Windows / PowerShell)

  La parte cloud (WM_Central + Kafka + SQLite) esta en Railway. Este script lanza en el PC
  del laboratorio los modulos WM_WS (monitor + engine) y WM_FO, cada uno en su propia ventana
  y dentro de Docker (no hace falta ningun IDE ni recompilar).

  Primero edita lab.env con las direcciones de Railway (CENTRAL y BROKER).

  Uso:
    .\wm.ps1 construir                                  # construye la imagen (una vez, antes de la correccion)
    .\wm.ps1 ws       WS-01 "Parque Norte"              # monitor + engine de una WS (2 ventanas)
    .\wm.ps1 monitor  WS-01 ["Parque Norte"]            # solo el monitor (p.ej. tras simular su caida)
    .\wm.ps1 engine   WS-01                             # solo el engine
    .\wm.ps1 fo       FO-01 [peticiones.txt]            # operario (con fichero opcional)
    .\wm.ps1 caer     wm-ws-01-e                        # simula un crash (mata el contenedor)
    .\wm.ps1 estado                                     # contenedores en marcha
    .\wm.ps1 parar                                      # para todos los contenedores wm-*
    .\wm.ps1 local                                      # (pruebas en casa) Kafka + Central en local con docker compose
    .\wm.ps1 panel                                      # abre el panel web de la Central

  Sin ubicacion, el monitor solo se autentica: si la WS no esta registrada, Central la DENIEGA.
#>
param(
    [Parameter(Position = 0)][string]$Accion = "ayuda",
    [Parameter(Position = 1)][string]$Id,
    [Parameter(Position = 2, ValueFromRemainingArguments = $true)][string[]]$Resto
)

$ErrorActionPreference = "Stop"
$Raiz = $PSScriptRoot
$Imagen = "wm-sd"
$Red = "wm-lab"

function Leer-Config {
    $cfg = @{ CENTRAL = "host.docker.internal:8080"; BROKER = "host.docker.internal:9092"; PANEL = "http://localhost:8000" }
    $f = Join-Path $Raiz "lab.env"
    if (Test-Path $f) {
        foreach ($l in Get-Content $f) {
            if ($l -match '^\s*([A-Z_]+)\s*=\s*(.+?)\s*$') { $cfg[$Matches[1]] = $Matches[2] }
        }
    }
    return $cfg
}

function Asegurar-Docker {
    $ErrorActionPreference = "Continue"   # docker inspect falla a proposito si la imagen/red no existen
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { throw "Docker no esta arrancado. Abre Docker Desktop y vuelve a intentarlo." }
    docker image inspect $Imagen *> $null
    if ($LASTEXITCODE -ne 0) { Write-Host "La imagen $Imagen no existe todavia: construyendo..." -ForegroundColor Yellow; Construir }
    docker network inspect $Red *> $null
    if ($LASTEXITCODE -ne 0) { docker network create $Red | Out-Null }
}

function Construir {
    Write-Host "Construyendo imagen $Imagen (compila WM_Central, WM_WS y WM_FO)..." -ForegroundColor Cyan
    docker build -t $Imagen $Raiz
    if ($LASTEXITCODE -ne 0) { throw "Fallo al construir la imagen" }
    Write-Host "Imagen $Imagen lista." -ForegroundColor Green
}

# Abre una ventana nueva con el contenedor en modo interactivo (-it) para poder usar su menu.
function Abrir-Ventana([string]$Titulo, [string]$Nombre, [string]$ArgsDocker) {
    $cmd = "`$host.UI.RawUI.WindowTitle = '$Titulo'; " +
           "docker rm -f $Nombre 2>`$null | Out-Null; " +
           "docker run --rm -it --init --name $Nombre --network $Red $ArgsDocker; " +
           "Write-Host ''; Write-Host '[$Titulo] proceso terminado. Cierra la ventana o relanzalo con wm.ps1.' -ForegroundColor Yellow"
    $enc = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($cmd))
    Start-Process powershell -ArgumentList @("-NoExit", "-NoProfile", "-EncodedCommand", $enc) | Out-Null
    Write-Host "  -> ventana '$Titulo' (contenedor $Nombre)" -ForegroundColor Green
}

function Nombre-Ws([string]$ws) { return "wm-" + $ws.ToLower() }

function Lanzar-Monitor([string]$ws, [string]$ubicacion) {
    $c = Leer-Config
    $n = (Nombre-Ws $ws) + "-m"
    $ubi = if ($ubicacion) { " `"$ubicacion`"" } else { "" }
    Abrir-Ventana "$ws MONITOR" $n "$Imagen java -cp 'lib/*:clases/ws' WM_WS_M 5000 $($c.CENTRAL) $ws$ubi"
}

function Lanzar-Engine([string]$ws) {
    $c = Leer-Config
    $n = (Nombre-Ws $ws) + "-e"
    $datos = Join-Path $Raiz ("datos_ws\" + $ws)
    New-Item -ItemType Directory -Force $datos | Out-Null
    $monitor = (Nombre-Ws $ws) + "-m:5000"
    Abrir-Ventana "$ws ENGINE" $n "-v `"${datos}:/app/datos`" $Imagen java -cp 'lib/*:clases/ws' WM_WS_E $($c.BROKER) $monitor"
}

function Lanzar-FO([string]$fo, [string]$fichero) {
    $c = Leer-Config
    $n = "wm-" + $fo.ToLower()
    $extra = ""
    $arg = ""
    if ($fichero) {
        $ruta = (Resolve-Path $fichero).Path
        $dir = Split-Path $ruta -Parent
        $arg = " /app/entrada/" + (Split-Path $ruta -Leaf)
        $extra = "-v `"${dir}:/app/entrada:ro`" "
    }
    Abrir-Ventana "$fo OPERARIO" $n "$extra$Imagen java -cp 'lib/*:clases/fo' WM_FO $($c.BROKER) $fo$arg"
}

switch ($Accion.ToLower()) {
    "construir" { Construir }
    "ws" {
        if (-not $Id) { throw "Uso: .\wm.ps1 ws WS-01 [ubicacion]" }
        Asegurar-Docker
        $ubi = ($Resto -join " ").Trim()
        if (-not $ubi) { $ubi = "Zona $Id" }   # asi una WS nueva (WS-04, WS-05...) queda registrada sin mas
        Lanzar-Monitor $Id $ubi
        Start-Sleep -Seconds 2
        Lanzar-Engine $Id
    }
    "monitor" {
        if (-not $Id) { throw "Uso: .\wm.ps1 monitor WS-01 [ubicacion]" }
        Asegurar-Docker; Lanzar-Monitor $Id ($Resto -join " ")
    }
    "engine" {
        if (-not $Id) { throw "Uso: .\wm.ps1 engine WS-01" }
        Asegurar-Docker; Lanzar-Engine $Id
    }
    "fo" {
        if (-not $Id) { throw "Uso: .\wm.ps1 fo FO-01 [fichero]" }
        Asegurar-Docker; Lanzar-FO $Id ($Resto | Select-Object -First 1)
    }
    "caer" {
        if (-not $Id) { throw "Uso: .\wm.ps1 caer <contenedor>   (ver .\wm.ps1 estado)" }
        docker kill $Id | Out-Null
        Write-Host "Contenedor $Id parado de golpe (crash simulado)." -ForegroundColor Yellow
    }
    "estado" { docker ps --filter "name=wm-" --format "table {{.Names}}\t{{.Status}}" }
    "parar" {
        $ids = docker ps -aq --filter "name=wm-"
        if ($ids) { docker rm -f $ids | Out-Null }
        Write-Host "Contenedores wm-* parados." -ForegroundColor Yellow
    }
    "local" {
        docker compose -f (Join-Path $Raiz "docker-compose.yml") up -d --build
        Write-Host "Parte cloud en local: panel http://localhost:8000 | Central host.docker.internal:8080 | Kafka host.docker.internal:9092" -ForegroundColor Green
    }
    "panel" { Start-Process (Leer-Config).PANEL }
    default { Get-Content $PSCommandPath -TotalCount 24 | Select-Object -Skip 1 | Out-Host }
}
