package no.ssb.dapla.catalog;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JsonProtobufUtils {

    public static <T> String toString(T pojo) {
        if (MessageOrBuilder.class.isAssignableFrom(pojo.getClass())) {
            try {
                return JsonFormat.printer().print((MessageOrBuilder) pojo);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("class is not compatible with " + MessageOrBuilder.class.getName());
        }
    }

    public static <T> T toPojo(String json, Class<T> clazz) {
        if (MessageOrBuilder.class.isAssignableFrom(clazz)) {
            try {
                Method newBuilderMethod = clazz.getMethod("newBuilder", (Class<?>[]) null);
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
            throw new RuntimeException("class is not compatible with " + MessageOrBuilder.class.getName());
        }
    }
}
