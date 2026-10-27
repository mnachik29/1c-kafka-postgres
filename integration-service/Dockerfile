FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/integration-service.jar /app/integration-service.jar
# `docker compose exec integration-service ./sync --mode full`
RUN printf '#!/bin/sh\nexec java -jar /app/integration-service.jar sync "$@"\n' > /app/sync && chmod +x /app/sync
ENV STATE_DIR=/data
VOLUME /data
EXPOSE 8081
HEALTHCHECK --interval=15s --timeout=5s --retries=5 CMD curl -fs http://localhost:8081/health || exit 1
ENTRYPOINT ["java", "-jar", "/app/integration-service.jar"]
CMD ["serve"]
