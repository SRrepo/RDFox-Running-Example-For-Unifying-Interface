package RDFox;

public class DatastoreBatchUpdater implements Runnable{
    private boolean stop = false;
    private RDFoxWrapper r;

    public DatastoreBatchUpdater(RDFoxWrapper r) {
        this.r = r;
    }

    @Override
    public void run() {
        while (!stop) {
            r.updateDataStoreBatch();
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public void stop() {
        stop = true;
    }
}
