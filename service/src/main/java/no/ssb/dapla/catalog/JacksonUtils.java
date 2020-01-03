package no.ssb.dapla.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JacksonUtils {

    public static final ObjectMapper mapper = (new ObjectMapper()).registerModule(new ParameterNamesModule()).registerModule(new Jdk8Module()).registerModule(new JavaTimeModule());

    public static <T> String toString(T pojo) {
        if (MessageOrBuilder.class.isAssignableFrom(pojo.getClass())) {
            try {
                return JsonFormat.printer().print((MessageOrBuilder) pojo);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return mapper.writeValueAsString(pojo);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T toPojo(String json, Class<T> clazz) {
        if (MessageOrBuilder.class.isAssignableFrom(clazz)) {
            try {
                Method newBuilderMethod = clazz.getMethod("newBuilder", null);
                Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
                JsonFormat.parser().merge(json, builder);
                Message message = builder.build();
                if (clazz.isAssignableFrom(message.getClass())) {
                    return (T) message;
                } else {
                    throw new IllegalArgumentException("Incompatible types");
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                return mapper.readValue(json, clazz);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
