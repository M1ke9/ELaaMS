package ProduceData;

import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import Serdes.Init.DataStructureSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class TrainingProducer1 implements Runnable {

    private final String trainingTopic = "training-topic";
    private final String filePath;
    private final KafkaProducer<String, DataStructure> producer;
    private final ObjectMapper objectMapper;

   // private final int partition;  // ADDED: which partition to send to

    public TrainingProducer1(String filePath) {
        this.filePath = filePath;
        //this.partition = partition;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvironmentConfiguration.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DataStructureSerializer.class.getName());
        // If you have a custom partitioner, you could set it here.
        // e.g. props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.my.StreamIdPartitioner");

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void run() {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Convert JSON -> DataStructure
                DataStructure record = objectMapper.readValue(line, DataStructure.class);


              //  String key =record.getStreamID()+"-"+record.getDataSetKey();
                String key =record.getStreamID();
                // Send to the chosen partition
                producer.send(new ProducerRecord<>(
                        trainingTopic,                 // topic// partition index
                        key,          // key
                        record                         // value
                ));
                count++;
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("Sent " + count + " training records from file " + filePath
                    + " to topic '" + trainingTopic);
        }
    }
}
