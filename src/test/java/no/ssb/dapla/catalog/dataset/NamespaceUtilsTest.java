package no.ssb.dapla.catalog.dataset;

import org.junit.jupiter.api.Test;

import java.util.List;

import static no.ssb.dapla.catalog.dataset.NamespaceUtils.escapePath;
import static no.ssb.dapla.catalog.dataset.NamespaceUtils.unescapePath;
import static org.assertj.core.api.Assertions.assertThat;

public class NamespaceUtilsTest {

    @Test
    void testEscapePath() {
        var tests = List.of(
                "/some weird/p@th/to_escape",
                "/path/john.doe/home",
                "/!path",
                "/!path",
                "/!!path"
        );
        for (String test : tests) {
            assertThat(unescapePath(escapePath(test))).isEqualTo(test);
        }
    }

    @Test
    void testUnEscapePath() {
        assertThat(unescapePath("path")).isEqualTo("/path");
        assertThat(unescapePath(".a.path")).isEqualTo("/a/path");
        assertThat(unescapePath("a.path")).isEqualTo("/a/path");
        assertThat(unescapePath("a.path.")).isEqualTo("/a/path");
    }


}
