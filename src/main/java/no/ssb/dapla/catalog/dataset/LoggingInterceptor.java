package no.ssb.dapla.catalog.dataset;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class.getName());

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        LOG.trace("CALL: {}", call.getMethodDescriptor());
        return next.startCall(call, headers);
    }
}
