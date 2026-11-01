FROM openjdk:11-jdk-slim
WORKDIR /app

# Copiamos librerías (Kafka, SQLite)
COPY lib/ ./lib/

# Copiamos la nueva estructura de directorios
COPY WM_Central/ ./WM_Central/
COPY WM_WS/ ./WM_WS/
COPY WM_FO/ ./WM_FO/

# Compilamos indicando el classpath
RUN javac -cp ".:lib/*" WM_Central/*.java WM_WS/*.java WM_FO/*.java