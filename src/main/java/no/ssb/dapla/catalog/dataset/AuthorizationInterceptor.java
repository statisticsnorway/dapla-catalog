package no.ssb.dapla.catalog.dataset;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorizationInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationInterceptor.class.getName());

    static final ThreadLocal<String> tokenThreadLocal = new ThreadLocal<>();

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        LOG.trace("Intercepted grpc call, headers: {}", headers);
        String authorization = headers.get(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER));
        String token;
        if (authorization != null && authorization.startsWith("Bearer ")) {
            token = authorization.substring("Bearer ".length());
        } else {
            token = "no-grpc-token";
        }
        tokenThreadLocal.set(token);
        return next.startCall(call, headers);
    }
}
