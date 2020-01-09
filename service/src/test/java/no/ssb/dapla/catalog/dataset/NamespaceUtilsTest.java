package no.ssb.dapla.catalog.dataset;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NamespaceUtilsTest {

    @Test
    public void toComponents() {
        List<String> components = NamespaceUtils.toComponents("/c1/c2/c3");
        assertEquals(List.of("c1", "c2", "c3"), components);
    }

    @Test
    public void toNamespace() {
        String namespace = NamespaceUtils.toNamespace(List.of("c1", "c2", "c3"));
        assertEquals("/c1/c2/c3", namespace);
    }
}