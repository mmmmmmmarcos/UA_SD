# Imagen unica de WaterManagement (WM_Central, WM_WS_M, WM_WS_E y WM_FO).
#  - En Railway se usa para el servicio "central" (CMD por defecto).
#  - En el laboratorio se usa para lanzar monitores, engines y operarios (scripts wm.ps1 / wm.sh).
# El codigo se compila UNA vez al construir la imagen; en la correccion solo se ejecuta.

FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /src
COPY lib/ lib/
COPY WM_Central/ WM_Central/
COPY WM_WS/ WM_WS/
COPY WM_FO/ WM_FO/
RUN javac -encoding UTF-8 -cp "lib/*" -d /out/central WM_Central/*.java \
 && javac -encoding UTF-8 -cp "lib/*" -d /out/ws      WM_WS/*.java \
 && javac -encoding UTF-8 -cp "lib/*" -d /out/fo      WM_FO/*.java

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY lib/ lib/
COPY --from=build /out/ clases/
COPY WM_Central/web/ web/
COPY WM_FO/peticiones.txt ./

# Parametros de WM_Central (se pueden cambiar con variables de entorno en Railway)
ENV PUERTO_SOCKET=8080 \
    KAFKA_BROKER=kafka.railway.internal:29092 \
    PORT=8000 \
    DB_PATH=/data/riego_sd.db

EXPOSE 8080 8000
CMD ["sh", "-c", "exec java -cp 'lib/*:clases/central' WM_Central $PUERTO_SOCKET $KAFKA_BROKER $PORT $DB_PATH"]
