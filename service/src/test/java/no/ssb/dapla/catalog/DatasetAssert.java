package no.ssb.dapla.catalog;

import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.protobuf.Dataset;
import org.assertj.core.api.AbstractAssert;

import java.util.List;

public class DatasetAssert extends AbstractAssert<DatasetAssert, Dataset> {
    public DatasetAssert(Dataset dataset) {
        super(dataset, DatasetAssert.class);
    }

    public static DatasetAssert assertThat(Dataset actual) {
        return new DatasetAssert(actual);
    }

    public DatasetAssert isEqualTo(Dataset expected) {
        if (actual == expected) {
            return this;
        }
        if (actual == null) {
            failWithMessage("Expected:\n    %s\nbut got:\n  null", expected);
        }
        if (expected == null) {
            failWithMessage("Expected:\n    null\nbut got:\n    %s", actual);
        }
        org.assertj.core.api.Assertions.assertThat(actual.getId()).isEqualTo(expected.getId());
        org.assertj.core.api.Assertions.assertThat(actual.getState()).isEqualTo(expected.getState());
        org.assertj.core.api.Assertions.assertThat(actual.getValuation()).isEqualTo(expected.getValuation());
        List<ByteString> actualList = this.actual.getLocationsList().asByteStringList();
        List<ByteString> expectedList = expected.getLocationsList().asByteStringList();
        org.assertj.core.api.Assertions.assertThat(actualList).hasSameSizeAs(expectedList);
        for (ByteString expectedString : expectedList) {
            org.assertj.core.api.Assertions.assertThat(actualList).contains(expectedString);
        }
        return this;
    }
}
