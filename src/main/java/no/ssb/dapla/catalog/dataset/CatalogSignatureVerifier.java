package no.ssb.dapla.catalog.dataset;

public interface CatalogSignatureVerifier {

    boolean verify(byte[] data, byte[] receivedSign);
}