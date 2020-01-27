package no.ssb.dapla.catalog.dataset;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.opentracing.Span;
import no.ssb.helidon.application.Tracing;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class AuthorizationInterceptor implements ServerInterceptor {

    static final ThreadLocal<String> tokenThreadLocal = new ThreadLocal<>();

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Span span = Tracing.spanFromGrpc(null, "AuthorizationInterceptor");
        try {
            String authorization = headers.get(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER));
            String token;
            if (authorization != null && authorization.startsWith("Bearer ")) {
                token = authorization.substring("Bearer ".length());
                String[] parts = token.split("[.]");
                if (parts.length == 3) {
                    String header = new String(Base64.getDecoder().decode(parts[0]), StandardCharsets.UTF_8);
                    String payload = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8);
                    span.log(Map.of("event", "token", "header", header, "payload", payload));
                } else {
                    span.log(Map.of("event", "invalid token", "token", token));
                }
            } else {
                token = "no.grpc.token";
            }
            tokenThreadLocal.set(token);
            return next.startCall(call, headers);
        } finally {
            span.finish();
        }
    }
}
