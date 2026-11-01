FROM eclipse-temurin:11-jdk-jammy
WORKDIR /app

COPY lib/ ./lib/

COPY WM_Central/ ./WM_Central/
COPY WM_WS/ ./WM_WS/
COPY WM_FO/ ./WM_FO/

RUN javac -encoding ISO-8859-1 -cp ".:lib/*" WM_Central/*.java WM_WS/*.java WM_FO/*.java