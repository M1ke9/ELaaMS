package ProduceData;



import Structure.DataStructure;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class PredictionProducer11 implements Runnable {

    private final String predictionTopic = "prediction-topic";
    private final String filePath;
    // Use the shared producer instance:
    private final KafkaProducer<String, DataStructure> producer;
    private final ObjectMapper objectMapper;

   // private final int partition;

    public PredictionProducer11(String filePath) {
        this.filePath = filePath;
        this.producer = ProducerSingleton.getInstance();
        this.objectMapper = new ObjectMapper();
      //  this.partition=partition;
    }

    @Override
    public void run() {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Convert JSON -> DataStructure
                DataStructure record = objectMapper.readValue(line, DataStructure.class);


               String key = record.getStreamID();

               // String key = record.getStreamID()+"-"+ record.getDataSetKey();
                // Send the record
                producer.send(new ProducerRecord<>(predictionTopic, null, record));
                count++;
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Sent " + count + " prediction records from file " + filePath
                    + " to topic '" + predictionTopic + "'");
            // Do NOT close the shared producer here!
        }
    }
}
