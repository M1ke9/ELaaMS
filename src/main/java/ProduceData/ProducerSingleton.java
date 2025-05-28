package ProduceData;



import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import Serdes.Init.DataStructureSerializer;

import java.util.Properties;

public class ProducerSingleton {
    private static KafkaProducer<String, DataStructure> instance;

    public static synchronized KafkaProducer<String, DataStructure> getInstance() {
        if (instance == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvironmentConfiguration.getBootstrapServers());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DataStructureSerializer.class.getName());
            instance = new KafkaProducer<>(props);
        }
        return instance;
    }

    public static synchronized void close() {
        if (instance != null) {
            instance.close();
            instance = null;
        }
    }
}
