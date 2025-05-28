package ProduceData;

import Configuration.EnvironmentConfiguration;
import Structure.ControlStructure;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import Serdes.Init.ControlStructureSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class ControlCommandProducer {

    private static final String CONTROL_TOPIC = "control-topic";
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Reads the JSON file at {@code controlJsonFilePath} into a list of ControlStructure,
     * then produces each ControlStructure to the 'control-topic' using the custom serializer.
     */
    public void sendControlCommandsFromFile(String controlJsonFilePath) {

        try (KafkaProducer<String, ControlStructure> controlProducer = createControlProducer()) {

            // 1) Read the JSON array from file
            String fileContent = new String(Files.readAllBytes(Paths.get(controlJsonFilePath)));
            System.out.println("Processing file: " + controlJsonFilePath);

            // 2) Deserialize into list of ControlStructure
            List<ControlStructure> commands = objectMapper.readValue(
                    fileContent,
                    new TypeReference<List<ControlStructure>>() {}
            );

            // 3) Produce each ControlStructure to Kafka
            for (ControlStructure command : commands) {
                String key = command.getStreamID() + "-" + command.getDataSetKey();

                // Current code sends to partition 0, as per the original.
                // If you want default round-robin or key-based partitioning, remove the '0' from the constructor.
                // Example for round-robin/key-based: new ProducerRecord<>(CONTROL_TOPIC, key, command);
                ProducerRecord<String, ControlStructure> record = new ProducerRecord<>(CONTROL_TOPIC, null, command);

                controlProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Sent message from file " + controlJsonFilePath +
                                " to partition " + metadata.partition() +
                                " with offset " + metadata.offset());
                    }
                });
            }

            // 4) Flush to ensure messages are actually sent before closing
            controlProducer.flush();
            System.out.println("Finished sending commands from file: " + controlJsonFilePath);

        } catch (IOException e) {
            System.err.println("Error processing file " + controlJsonFilePath + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Reads all JSON files from the specified directory and sends their control commands to Kafka.
     * Only files with a ".json" extension are considered.
     *
     * @param directoryPath The path to the directory containing JSON files.
     */
    public void sendControlCommandsFromDirectory(String directoryPath) {
        System.out.println("Processing directory: " + directoryPath);
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            paths
                    .filter(Files::isRegularFile) // Only consider regular files
                    .filter(path -> path.toString().endsWith(".json")) // Filter for JSON files
                    .forEach(jsonFilePath -> {
                        System.out.println("Found JSON file: " + jsonFilePath);
                        sendControlCommandsFromFile(jsonFilePath.toString()); // Reuse the existing method
                    });
        } catch (IOException e) {
            System.err.println("Error reading directory " + directoryPath + ": " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Finished processing directory: " + directoryPath);
    }

    /**
     * Creates a KafkaProducer that uses String for the key and ControlStructure for the value.
     */
    private KafkaProducer<String, ControlStructure> createControlProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Key = String; Value = our custom SerDe for ControlStructure
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ControlStructureSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        ControlCommandProducer producer = new ControlCommandProducer();

        // --- Option 1: Send a single JSON file (existing functionality) ---
        System.out.println("\n--- Sending commands from a single file ---");
       // String singleFilePath = EnvironmentConfiguration.getFilePathForControlTopic();
        //producer.sendControlCommandsFromFile(singleFilePath);


        // --- Option 2: Send all JSON files from a directory (new functionality) ---
        System.out.println("\n--- Sending commands from a directory ---");
        String directoryWithControlJsonFiles = EnvironmentConfiguration.getFilePathForControlTopic(); // Example path. Adjust as needed.
        // Or if you have a way to configure this directory:
        // String directoryWithJsonFiles = EnvironmentConfiguration.getDirectoryPathForControlTopicData();
        producer.sendControlCommandsFromDirectory(directoryWithControlJsonFiles);
    }
}