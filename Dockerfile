FROM gradle:8.5-jdk11 AS builder

WORKDIR /build

COPY gradle/ gradle/
COPY gradlew gradlew.bat gradle.properties settings.gradle.kts build.gradle.kts ./
COPY stream-common/ stream-common/
COPY ssp-stream/ ssp-stream/
COPY dsp-stream/ dsp-stream/

RUN gradle :ssp-stream:shadowJar :dsp-stream:shadowJar --no-daemon --info

FROM flink:1.18.1-scala_2.12-java11

WORKDIR /opt/flink/usrlib

COPY --from=builder /build/ssp-stream/build/libs/ssp-stream-*-all.jar ./ssp-stream.jar
COPY --from=builder /build/dsp-stream/build/libs/dsp-stream-*-all.jar ./dsp-stream.jar

USER flink
