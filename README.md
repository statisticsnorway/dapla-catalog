# dapla-catalog
Catalog reactor. The catalog domain maintains headers, metadata, and schema of datasets

Add the following jvm options when running tests or application with module-system:
```
--add-exports=io.grpc/io.opencensus.common=gax
--add-exports=io.grpc/io.opencensus.trace=gax
--add-exports=io.grpc/io.opencensus.tags=google.cloud.bigtable
--add-exports=io.grpc/io.opencensus.stats=google.cloud.bigtable
--add-exports=io.grpc/io.opencensus.trace=com.google.api.client
--add-exports=io.grpc/io.opencensus.trace.propagation=com.google.api.client
--add-exports=io.grpc/io.opencensus.trace.propagation=opencensus.contrib.http.util
```

