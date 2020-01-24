package no.ssb.dapla.catalog.dataset;

import io.helidon.common.context.Contexts;
import io.helidon.webserver.ServerRequest;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class Tracing {

    public static Span spanFromGrpc(String operationName) {
        SpanContext spanContext = Contexts.context().orElseThrow().get(SpanContext.class).get();
        Tracer tracer = GlobalTracer.get();
        Span span = tracer
                .buildSpan(operationName)
                .asChildOf(spanContext)
                .start();
        tracer.scopeManager().activate(span);
        return span;
    }

    public static Span spanFromHttp(ServerRequest request, String operationName) {
        Span span = request.tracer()
                .buildSpan(operationName)
                .asChildOf(request.spanContext())
                .start();
        request.tracer().scopeManager().activate(span);
        return span;
    }

    public static void logError(Span span, Throwable e, String event) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        span.log(Map.of("event", event, "message", e.getMessage(), "stacktrace", stringWriter.toString()));
    }
}
