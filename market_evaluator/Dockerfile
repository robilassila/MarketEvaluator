# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src src
RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --no-transfer-progress -Dmaven.test.skip=true \
        compile dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib

FROM eclipse-temurin:17-jre
COPY --from=build /build/target/lib /app/lib
COPY --from=build /build/target/classes /app/classes
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/evaluator
WORKDIR /workspace
ENTRYPOINT ["evaluator"]
