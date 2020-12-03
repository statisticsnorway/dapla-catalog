FROM eu.gcr.io/prod-bip/alpine-jdk15-buildtools:master-7744b1c6a23129ceaace641d6d76d0a742440b58 as build

#
# Build stripped JVM
#
RUN ["jlink", "--strip-java-debug-attributes", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/jdk/jmods", "--output", "/linked",\
 "--add-modules", "java.base,java.management,jdk.unsupported,java.sql,jdk.zipfs,jdk.naming.dns,java.desktop,java.net.http,jdk.crypto.cryptoki,jdk.jcmd,jdk.jartool,jdk.jdi,jdk.jfr"]

#
# Build Application image
#
FROM alpine:latest

RUN apk --no-cache add curl tar gzip nano jq

#
# Resources from build image
#
COPY --from=build /linked /jdk/
COPY --from=build /opt/jdk/bin/jar /opt/jdk/bin/jcmd /opt/jdk/bin/jdb /opt/jdk/bin/jfr /opt/jdk/bin/jinfo /opt/jdk/bin/jmap /opt/jdk/bin/jps /opt/jdk/bin/jstack /opt/jdk/bin/jstat /jdk/bin/
COPY run.sh /app/
COPY target/libs /app/lib/
COPY target/catalog-service*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/

ENV PATH=/jdk/bin:$PATH

WORKDIR /app

EXPOSE 10110

CMD ["./run.sh"]
