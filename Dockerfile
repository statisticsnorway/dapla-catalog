FROM statisticsnorway/alpine-jdk13-buildtools:latest as build

#
# Build stripped JVM
#
RUN ["jlink", "--strip-java-debug-attributes", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/jdk/jmods", "--output", "/linked",\
 "--add-modules", "java.base,java.management,jdk.unsupported,java.sql,jdk.zipfs,jdk.naming.dns,java.desktop,java.net.http,jdk.crypto.cryptoki"]

#
# Build Application image
#
FROM alpine:latest

#
# Resources from build image
#
COPY --from=build /linked /jdk/
COPY run.sh /app/
COPY target/libs /app/lib/
COPY target/catalog-service*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/
COPY src/test/resources/catalog-verifier_keystore.p12 /app/secret/

ENV PATH=/jdk/bin:$PATH

WORKDIR /app

EXPOSE 10110
EXPOSE 10118

CMD ["./run.sh"]
