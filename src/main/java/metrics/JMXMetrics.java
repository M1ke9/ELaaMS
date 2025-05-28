package metrics;


import javax.management.remote.JMXServiceURL;
import java.util.ArrayList;
import java.util.List;

public abstract class JMXMetrics {
    protected List<JMXServiceURL> serviceUrls = new ArrayList<>();
    public static int STANDARD_PERIOD=0;
    public String fileName;//need changing

    public void addServiceUrl(String url) throws Exception {
        serviceUrls.add(new JMXServiceURL(url));
    }

    public abstract void collectMetrics() throws Exception;
}