package IO.Serialization.QueryInformation;

public class PhysicalWindow extends Window {
    int windowSize;

    public PhysicalWindow(String streamURL, int windowSize) {
        super(streamURL);
        this.windowSize = windowSize;
    }
}
