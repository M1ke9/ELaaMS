package Configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

public class CreateConfiguration {

    public Properties getPropertiesForMicroservice(String APPLICATION_ID , String BOOTSTRAP_SERVERS) {
        // KafkaConfigLoader configLoader = new KafkaConfigLoader();
        // try {

        Properties properties = new Properties();
        //properties = configLoader.loadProperties(EnvironmentConfiguration.getFilePathForPropertiesfile());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID); // application id for kafka streams (in this name based the group id for kafka consumer)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // kafka bootstrap server this contains the broker address and ports (in this case we have 3 brokers)
        //properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);  // Setting number of standby replicas to 1 because we want to run only two instance(n the number of replicas stands by and n+1 the instance that is running)
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest means that the consumer will read from the beginning of the topic
        //properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, EnvironmentConfiguration.giveTheReplicationFactor()); // replication factor 2 this means that we have 2 replicas for each partition and distributed alongside the brokers
        properties.put(StreamsConfig.STATE_DIR_CONFIG, EnvironmentConfiguration.getTempDir()); // state directory for storing the state of the kafka streams
        //ForTesting temporary disable
        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        return properties;

        // } catch (IOException e) {
        //     e.printStackTrace();
        // Handle exception
        //   }


        //properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");




        //return null;
    }

    public Properties getPropertiesConfig(Class<?> valueSerializerClass) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", EnvironmentConfiguration.getBootstrapServers());
        properties.put("acks","all");
        properties.put("retries", 3);

        //for sending batxhes
        properties.put("batch.size", 16384);  // Set batch size to 16 KB
        //properties.put("linger.ms", 10);  // Wait up to 10 ms for additional messages
        //properties.put("buffer.memory", 10);  // Wait up to 10 ms for additional messages

        //temporary disable
        //properties.put("compression.type", "gzip");  // Enable compression

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", valueSerializerClass.getName());
        return properties;
    }

}