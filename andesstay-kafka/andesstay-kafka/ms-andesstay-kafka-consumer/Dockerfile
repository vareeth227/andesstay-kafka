# Dockerfile de ms-andesstay-kafka-consumer
# (renombrar a "Dockerfile" dentro de la carpeta del microservicio)
# Multi-stage: etapa 1 compila con Maven, etapa 2 solo corre el jar.

# ---- Etapa 1: build ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q clean package -DskipTests

# ---- Etapa 2: runtime ----
FROM eclipse-temurin:17-jre
WORKDIR /app
# Copia el jar generado (sin depender del nombre exacto)
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
