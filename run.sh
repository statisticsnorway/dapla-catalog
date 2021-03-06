#!/usr/bin/env sh

export JPMS_SWITCHES="
  --add-exports=io.grpc/io.opencensus.common=gax
  --add-exports=io.grpc/io.opencensus.trace=gax
  --add-exports=io.grpc/io.opencensus.trace=com.google.api.client
  --add-exports=io.grpc/io.opencensus.trace.propagation=com.google.api.client
  --add-exports=io.grpc/io.opencensus.trace.export=com.google.api.client
  --add-exports=io.grpc/io.opencensus.common=com.google.api.client
  --add-exports=io.grpc/io.opencensus.trace.propagation=opencensus.contrib.http.util
  --add-exports=io.grpc/io.opencensus.trace=opencensus.contrib.http.util
"

if [ "$JAVA_MODULE_SYSTEM_ENABLED" == "true" ]; then
  echo "Starting java using MODULE-SYSTEM"
  java $JPMS_SWITCHES -p /app/lib -m no.ssb.dapla.catalog/no.ssb.dapla.catalog.CatalogApplication
else
  echo "Starting java using CLASSPATH"
  java -cp "/app/lib/*" no.ssb.dapla.catalog.CatalogApplication
fi
