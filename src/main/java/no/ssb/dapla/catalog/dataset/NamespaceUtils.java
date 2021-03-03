package no.ssb.dapla.catalog.dataset;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NamespaceUtils {

    private static final Pattern CODEPOINT = Pattern.compile("_[0-9]{4}");
    private static final Pattern VALID_CHARS = Pattern.compile("([^\\w]|_)+");

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

    /**
     * Unescape all characters that could cause problems with the ltree column.
     */
    public static String unescapePath(String path) {
        var escaped = Stream.of(path.split("\\."))
                .map(part -> {
                    return CODEPOINT.matcher(part).replaceAll(match -> {
                        var point = Integer.parseInt(match.group().substring(1, 5));
                        return String.valueOf(Character.toChars(point));
                    });
                })
                .collect(Collectors.joining("/"));
        if (!escaped.startsWith("/")) {
            escaped = "/" + escaped;
        }
        return escaped;
    }

    /**
     * Escape all characters that could cause problems with the ltree column.
     */
    public static String escapePath(String path) {
        var escaped = Stream.of(path.split("/"))
                .map(part -> {
                    return VALID_CHARS.matcher(part).replaceAll(match -> {
                        return match.group().codePoints()
                                .mapToObj(codePoint -> String.format("!%04d", codePoint))
                                .collect(Collectors.joining());
                    });
                })
                .collect(Collectors.joining("."));
        if (escaped.startsWith(".")) {
            escaped = escaped.substring(1);
        }
        return escaped.replaceAll("!", "_");
    }

}
