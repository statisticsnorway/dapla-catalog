package no.ssb.dapla.catalog.dataset;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultCatalogSignatureVerifierTest {

    DefaultCatalogSignatureVerifier catalogSignatureVerifier = new DefaultCatalogSignatureVerifier(
            "PKCS12",
            "src/test/resources/catalog-verifier_keystore.p12",
            "dataAccessCertificate",
            "changeit".toCharArray(),
            "SHA256withRSA"
    );

    //@Test
    public void thatCatalogVerifyWorks() throws IOException {
        verifyDatasets("../localstack/data/datastore",
                "/ske/sirius-person-utkast/2018v19/1583156472183",
                "/skatt/person/rawdata-2019/1582719098762"
        );
    }

    private void verifyDatasets(String dataFolder, String... paths) throws IOException {
        for (String path : paths) {
            boolean valid = verify(dataFolder + path + "/.dataset-meta.json",
                    dataFolder + path + "/.dataset-meta.json.sign"
            );
            assertThat(valid).isTrue();
        }
    }

    private boolean verify(String fileToVerify, String signatureFile) throws IOException {
        byte[] data = Files.readAllBytes(Path.of(fileToVerify));
        byte[] providedSignature = Files.readAllBytes(Path.of(signatureFile));
        boolean valid = catalogSignatureVerifier.verify(data, providedSignature);
        return valid;
    }
}
