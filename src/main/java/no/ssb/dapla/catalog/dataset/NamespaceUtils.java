package no.ssb.dapla.catalog.dataset;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NamespaceUtils {

    public static String normalize(String name) {
        return NamespaceUtils.toNamespace(NamespaceUtils.toComponents(name));
    }

    public static List<String> toComponents(String name) {
        String source = name;
        while (source.startsWith("/")) {
            source = source.substring(1);
        }
        String[] parts = source.split("/");
        return Arrays.asList(parts);
    }

    public static String toNamespace(List<String> components) {
        return "/" + components.stream().collect(Collectors.joining("/"));
    }
}
