<#
  Lanza varias WS (monitor + engine) y varios operarios de golpe, cada uno en su ventana.
  Usa wm.ps1 por debajo, asi que lee las direcciones de Railway de lab.env.

  Ejemplos:
    .\wm-multi.ps1 -WS 3 -FO 2                       # WS-01..WS-03 y FO-01..FO-02
    .\wm-multi.ps1 -WS 2 -WSInicio 4                 # WS-04 y WS-05
    .\wm-multi.ps1 -FO 5 -Peticiones WM_FO\peticiones.txt   # 5 operarios leyendo el fichero
    .\wm-multi.ps1 -Parar                            # para todos los contenedores wm-*

  Las WS 1-3 usan su ubicacion registrada (Parque Norte, Jardin Central, Parque Sur);
  las demas se registran como "Zona WS-xx".
#>
param(
    [int]$WS = 0,
    [int]$FO = 0,
    [int]$WSInicio = 1,
    [int]$FOInicio = 1,
    [string]$Peticiones = "",
    [switch]$Parar
)

$ErrorActionPreference = "Stop"
$wm = Join-Path $PSScriptRoot "wm.ps1"
$conocidas = @{ 1 = "Parque Norte"; 2 = "Jardin Central"; 3 = "Parque Sur" }

if ($Parar) { & $wm parar; return }
if ($WS -le 0 -and $FO -le 0) { Get-Content $PSCommandPath -TotalCount 15 | Select-Object -Skip 1 | Out-Host; return }

# Construye la imagen una sola vez (wm.ps1 la construye si falta, pero asi no se repite por ventana)
docker image inspect wm-sd *> $null
if ($LASTEXITCODE -ne 0) { & $wm construir }

for ($i = $WSInicio; $i -lt $WSInicio + $WS; $i++) {
    $id = "WS-{0:00}" -f $i
    if ($conocidas.ContainsKey($i)) { & $wm ws $id $conocidas[$i] } else { & $wm ws $id }
    Start-Sleep -Seconds 1
}

for ($i = $FOInicio; $i -lt $FOInicio + $FO; $i++) {
    $id = "FO-{0:00}" -f $i
    if ($Peticiones) { & $wm fo $id $Peticiones } else { & $wm fo $id }
    Start-Sleep -Milliseconds 500
}

Write-Host "Lanzado: $WS WS y $FO operarios. Estado: .\wm.ps1 estado | Parar: .\wm-multi.ps1 -Parar" -ForegroundColor Green
