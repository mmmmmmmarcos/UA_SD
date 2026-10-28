FROM eclipse-temurin:17-jdk
WORKDIR /app
COPY lib/ ./lib/
COPY src/ ./src/
COPY peticiones.txt ./peticiones.txt
RUN javac -encoding ISO-8859-1 -cp ".:lib/*" src/*.java