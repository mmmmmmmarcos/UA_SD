#!/bin/sh
# WaterManagement - script de despliegue del LABORATORIO (Linux / macOS)
# Equivalente a wm.ps1. Edita lab.env con las direcciones de Railway.
#
#   ./wm.sh construir
#   ./wm.sh ws      WS-01 "Parque Norte"   # abre monitor + engine en terminales nuevas (si hay gnome-terminal/xterm)
#   ./wm.sh monitor WS-01 ["Parque Norte"] # en ESTA terminal
#   ./wm.sh engine  WS-01                  # en ESTA terminal
#   ./wm.sh fo      FO-01 [peticiones.txt] # en ESTA terminal
#   ./wm.sh caer    wm-ws-01-e             # crash simulado
#   ./wm.sh estado | parar | local
set -e
RAIZ="$(cd "$(dirname "$0")" && pwd)"
IMAGEN=wm-sd
RED=wm-lab
CENTRAL=host.docker.internal:8080
BROKER=host.docker.internal:9092
[ -f "$RAIZ/lab.env" ] && eval "$(grep -E '^[A-Z_]+=' "$RAIZ/lab.env" | sed 's/\r$//')"

preparar() {
  docker image inspect $IMAGEN >/dev/null 2>&1 || docker build -t $IMAGEN "$RAIZ"
  docker network inspect $RED >/dev/null 2>&1 || docker network create $RED >/dev/null
}
# --add-host para que host.docker.internal funcione tambien en Linux (pruebas locales)
ejecutar() { nombre=$1; shift; docker rm -f "$nombre" >/dev/null 2>&1 || true
  docker run --rm -it --init --add-host=host.docker.internal:host-gateway --name "$nombre" --network $RED "$@"; }
nueva_terminal() {
  if command -v gnome-terminal >/dev/null; then gnome-terminal --title="$1" -- sh -c "$2; exec sh"
  elif command -v xterm >/dev/null; then xterm -T "$1" -e sh -c "$2; exec sh" &
  else echo "Abre otra terminal y ejecuta: $2"; fi
}
ws_min() { echo "$1" | tr 'A-Z' 'a-z'; }

accion=${1:-ayuda}; [ $# -gt 0 ] && shift
case "$accion" in
  construir) docker build -t $IMAGEN "$RAIZ" ;;
  monitor) preparar; id=$1; shift
    ejecutar "wm-$(ws_min "$id")-m" $IMAGEN java -cp 'lib/*:clases/ws' WM_WS_M 5000 "$CENTRAL" "$id" "$@" ;;
  engine) preparar; id=$1; mkdir -p "$RAIZ/datos_ws/$id"
    ejecutar "wm-$(ws_min "$id")-e" -v "$RAIZ/datos_ws/$id:/app/datos" $IMAGEN \
      java -cp 'lib/*:clases/ws' WM_WS_E "$BROKER" "wm-$(ws_min "$id")-m:5000" ;;
  ws) preparar; id=$1; shift
    ubi="$*"; [ -z "$ubi" ] && ubi="Zona $id"
    nueva_terminal "$id MONITOR" "'$RAIZ/wm.sh' monitor $id $ubi"; sleep 2
    nueva_terminal "$id ENGINE" "'$RAIZ/wm.sh' engine $id" ;;
  fo) preparar; id=$1
    if [ -n "$2" ]; then f=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
      ejecutar "wm-$(ws_min "$id")" -v "$(dirname "$f"):/app/entrada:ro" $IMAGEN \
        java -cp 'lib/*:clases/fo' WM_FO "$BROKER" "$id" "/app/entrada/$(basename "$f")"
    else ejecutar "wm-$(ws_min "$id")" $IMAGEN java -cp 'lib/*:clases/fo' WM_FO "$BROKER" "$id"; fi ;;
  caer) docker kill "$1" >/dev/null && echo "Contenedor $1 parado de golpe (crash simulado)" ;;
  estado) docker ps --filter name=wm- --format 'table {{.Names}}\t{{.Status}}' ;;
  parar) ids=$(docker ps -aq --filter name=wm-); [ -n "$ids" ] && docker rm -f $ids >/dev/null; echo "Parados" ;;
  local) docker compose -f "$RAIZ/docker-compose.yml" up -d --build ;;
  *) sed -n '2,12p' "$0" ;;
esac
