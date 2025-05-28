package metrics;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
public class ConfigMetrics {
    private static final Properties properties = new Properties();

    private static final String Default_Location_DIR_PATH = "C:\\kafka_projects\\kafka_1\\Configuration\\MetricsConfig.properties";

    static {
        String final_Location_Path = System.getProperty("configMetricsFilePath");

        if (final_Location_Path == null) {
            final_Location_Path = Default_Location_DIR_PATH;
        }

        try (InputStream input = new FileInputStream(final_Location_Path)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            // Handle exception, e.g., by falling back to default values or terminating the application.
        }
    }

    public static String giveTheOutputFileName() {
        return properties.getProperty("OUTPUT_FILENAME","C:\\Dataset\\MetricsResults\\routerMetrics.txt");
    }
    public static int giveTheMetricsPeriod() {
        return Integer.parseInt(properties.getProperty("PERIOD","5"));
    }

    public static String giveTheURLs() {
        return properties.getProperty("JMX_URLS","service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
    }

    public static boolean enableCommunicationCostMetrics() {
        return Boolean.parseBoolean(properties.getProperty("COMMUNICATION_COST_ENABLE","false"));
    }
}
