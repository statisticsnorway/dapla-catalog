package no.ssb.dapla.catalog.health;

public class ReadinessSample {
    final boolean dbConnected;
    final long time;

    public ReadinessSample(boolean dbConnected, long time) {
        this.dbConnected = dbConnected;
        this.time = time;
    }
}
