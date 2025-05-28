package helperClasses;

public class DynamicCountHolder {

    private volatile long requiredCount;
    public long get() {

        return requiredCount;
    }
    public void set(long v) {
        requiredCount = v;
    }

}
