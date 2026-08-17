# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1: build the React + Vite frontend
# ---------------------------------------------------------------------------
FROM node:20-alpine AS frontend
WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ---------------------------------------------------------------------------
# Stage 2: build the Spring Boot jar with the frontend embedded as static
# resources. The frontend dist MUST be copied into
# backend/src/main/resources/static BEFORE `mvn package` runs, so the jar
# bundles the UI and serves it from the same origin on :8080.
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /backend
COPY backend/pom.xml ./
COPY --from=frontend /frontend/dist ./src/main/resources/static
COPY backend/src ./src
RUN mvn -B -q -DskipTests package

# ---------------------------------------------------------------------------
# Stage 3: minimal JRE runtime
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
ENV JAVA_TOOL_OPTIONS="-Xmx192m"
COPY --from=build /backend/target/*.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
