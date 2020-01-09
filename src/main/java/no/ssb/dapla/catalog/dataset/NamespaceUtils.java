package no.ssb.dapla.catalog.dataset;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NamespaceUtils {

    public static List<String> toComponents(String name) {
        String source = name;
        if (name.startsWith("/")) {
            source = name.substring(1);
        }
        String[] parts = source.split("/");
        return Arrays.asList(parts);
    }

    public static String toNamespace(List<String> components) {
        return "/" + components.stream().collect(Collectors.joining("/"));
    }
}
