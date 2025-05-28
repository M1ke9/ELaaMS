package helperClasses;

import Configuration.EnvironmentConfiguration;
import Structure.ControlStructure;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.concurrent.atomic.AtomicInteger;

public class CustomStreamPartitioner5 implements StreamPartitioner<String, ControlStructure> {
    private AtomicInteger nextPartitionIndex = new AtomicInteger(0);

    @Override
    public Integer partition(String topic, String key, ControlStructure value, int numPartitions) {
        int partitionIndex = nextPartitionIndex.getAndIncrement() % EnvironmentConfiguration.giveTheParallelDegree();
        if (nextPartitionIndex.get() >= numPartitions) {
            nextPartitionIndex.set(0);
        }

        System.out.println("INDEX: " +partitionIndex);
        return partitionIndex;
    }
}
