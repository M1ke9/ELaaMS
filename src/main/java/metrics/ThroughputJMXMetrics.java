package metrics;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ThroughputJMXMetrics extends JMXMetrics {
    protected static int period = 0;

    @Override
    public void collectMetrics() throws Exception {
        double totalThroughput = 0.0;
        double totalLatency = 0.0;
        int threadCount = 0;



        for (JMXServiceURL url : serviceUrls) {
            MBeanServerConnection mbsc = FunctionalitiesJMX.connectToJMX(url);

            ObjectName queryName = new ObjectName("kafka.streams:type=stream-thread-metrics,thread-id=*");
            Set<ObjectName> mbeanNames = mbsc.queryNames(queryName, null);
            System.out.println("Found " + mbeanNames.size() + " matching MBeans");


/*
            for (ObjectName mbean : mbeanNames) {
                totalThroughput += FunctionalitiesJMX.getAttribute(mbsc, mbean, "process-rate");
                totalLatency += FunctionalitiesJMX.getAttribute(mbsc, mbean, "process-latency-avg");
            }

       }

 */




            for (ObjectName mbean : mbeanNames) {
                // Extract the 'thread-id' key property
                String threadId = mbean.getKeyProperty("thread-id");
                if (threadId != null && threadId.contains("RouterMicroservice-")) {
                    // Only sum the Router's threads
                    totalThroughput += FunctionalitiesJMX.getAttribute(mbsc, mbean, "process-rate");
                    totalLatency   += FunctionalitiesJMX.getAttribute(mbsc, mbean, "process-latency-avg");
                    threadCount++;
                }
            }



            System.out.println();
            Set<ObjectName> names = mbsc.queryNames(new ObjectName("kafka.streams:*"), null);
            for(ObjectName n : names){
                System.out.println(n.toString());

            }
            System.out.println();

        }


          double avgLatency = (threadCount > 0) ? (totalLatency / threadCount) : 0.0;


        String result = "Period: " + period + "-" + (period+STANDARD_PERIOD) + "\n"+
                "Total Process Rate: " + totalThroughput + "\n" +
                "Total Average Latency: " + avgLatency + "\n" ;
        period += STANDARD_PERIOD;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(super.fileName+".txt", true))) {
            writer.write(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        String jmxUrls = ConfigMetrics.giveTheURLs();
        JMXMetrics.STANDARD_PERIOD = ConfigMetrics.giveTheMetricsPeriod();

        ThroughputJMXMetrics throughputJMXMetrics = new ThroughputJMXMetrics();
        throughputJMXMetrics.fileName =ConfigMetrics.giveTheOutputFileName();

        Arrays.stream(jmxUrls.split(",")).forEach(url -> {
            try {
                throughputJMXMetrics.addServiceUrl(url);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(() -> {
            try {
                System.out.println();
                System.out.println("New Throughput Metric Captured!!!");
                throughputJMXMetrics.collectMetrics();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, JMXMetrics.STANDARD_PERIOD, TimeUnit.SECONDS);


    }


    }

