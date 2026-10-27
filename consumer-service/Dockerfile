# Контекст сборки — корень репозитория (нужен каталог migrations/)
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY consumer-service/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY consumer-service/src ./src
RUN mvn -q -B package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/consumer-service.jar /app/consumer-service.jar
COPY migrations /app/migrations
EXPOSE 8082
HEALTHCHECK --interval=15s --timeout=5s --retries=5 CMD curl -fs http://localhost:8082/health || exit 1
ENTRYPOINT ["java", "-jar", "/app/consumer-service.jar"]
