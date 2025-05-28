package Microservices.Router;

import Configuration.EnvironmentConfiguration;
import Microservices.MLAlgorithmMicroService.GenericMLAlgorithmMicroservice;
import Serdes.MLAlgorithmSerializer;
import Structure.ControlStructure;
import mlAlgorithms.MLAlgorithm;
import helperClasses.CreateTopic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 This code is working for the control commands in case we round robin them among router instances.
 This way we achieve generally better distribution,but there's the risk of duplicate creation.
 In case of round robin the creation requests, delete requests are replicated in order to find the corresponding task that holds the microservice.
 */
public class AggregatorProcessor1 implements Processor<String, ControlStructure, String, MicroServiceInfo> {
    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();
    private static final ConcurrentHashMap<String, GenericMLAlgorithmMicroservice> localMicroMap = new ConcurrentHashMap<>();

    private static final String DEFAULT_MODEL_PATH = "/models/";
    private ProcessorContext<String, MicroServiceInfo> context;
    private final String storeName;
    private KeyValueStore<String, MicroServiceInfo> store;

    private final CreateTopic createTopic = new CreateTopic();

    public AggregatorProcessor1(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, MicroServiceInfo> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, ControlStructure> record) {
        ControlStructure command = record.value();
        // aggregatorKey => either "streamId#datasetKey" if streamId is non-empty, else datasetKey
        String aggregatorKey = buildAggregatorKey(command);


        MicroServiceInfo partialInfo = store.get(aggregatorKey);
        if (partialInfo == null) {
            System.out.println("INTO");
            partialInfo = new MicroServiceInfo();
            partialInfo.setStreamId(command.getStreamID());
            partialInfo.setDatasetKey(command.getDataSetKey());
            // Create the topics if needed
            createTopicsIfNotExists(aggregatorKey);
        }

        // We'll build a partial record that includes only the change
        MicroServiceInfo partialRecord = new MicroServiceInfo();
        partialRecord.setStreamId(command.getStreamID());
        partialRecord.setDatasetKey(command.getDataSetKey());

        boolean changed = false;

        if ("CREATE".equalsIgnoreCase(command.getCommandType())) {
            String microserviceKey = buildMicroserviceKey(command);
            if (!partialInfo.hasMicroserviceKey(microserviceKey)) {
                partialInfo.addMicroservice(
                        microserviceKey,
                        command.getAlgorithmType(),
                        command.getAlgorithmID(),
                        command.getTarget(),
                        command.getTaskType(),
                        command.getHyperParams()
                );
                changed = true;

                // Physically start the local microservice
                MicroserviceDetails createdDetails = partialInfo.getMicroservices().get(microserviceKey);
                String microserviceId = createdDetails.getMicroserviceId();

                if (!localMicroMap.containsKey(microserviceId)) {
                    GenericMLAlgorithmMicroservice ms = new GenericMLAlgorithmMicroservice(
                            command.getAlgorithmType(),
                            command.getAlgorithmID(),
                            aggregatorKey, // you could store aggregatorKey or just streamId
                            command.getTarget(),
                            command.getTaskType(),
                            microserviceId,
                            command.getHyperParams()

                    );
                    ms.clear();
                    ms.start();
                    localMicroMap.put(microserviceId, ms);

                    String TopicName = "EnsembleTopicForData";

                    createTopic.createTopicsForStreamIdAndTargetIfNotExists(TopicName, aggregatorKey, command.getTarget());

                    System.out.printf("*** Created microservice => aggregatorKey=%s, microserviceKey=%s%n",
                            aggregatorKey, microserviceKey);
                }

                createdDetails.setRemoved(false);
                partialRecord.getMicroservices().put(microserviceKey, createdDetails);

              //  System.out.printf("Created membership => aggregatorKey=%s, microserviceKey=%s%n",
                      //  aggregatorKey, microserviceKey);
            }
        }
        else if ("DELETE".equalsIgnoreCase(command.getCommandType())) {
            String microserviceKey = buildMicroserviceKey(command);
            if (partialInfo.hasMicroserviceKey(microserviceKey)) {
                MicroserviceDetails existing = partialInfo.getMicroservices().get(microserviceKey);
                String microserviceId = existing.getMicroserviceId();

                // Physically stop
                GenericMLAlgorithmMicroservice ms = localMicroMap.remove(microserviceId);
                if (ms != null) {
                    ms.stop();
                    ms.clear();
                    System.out.printf("*** Stopped microservice => aggregatorKey=%s, microserviceKey=%s%n",
                            aggregatorKey, microserviceKey);
                }

                partialInfo.removeMicroservice(microserviceKey);
                changed = true;

                existing.setRemoved(true);
                partialRecord.getMicroservices().put(microserviceKey, existing);
            }
        }
        else if ("LOAD".equalsIgnoreCase(command.getCommandType())) {

            KafkaProducer<String, MLAlgorithm> aggregatorProducer =new  KafkaProducer<>(getProducerProps());
            String microserviceKey = buildMicroserviceKey(command);
            if (partialInfo.hasMicroserviceKey(microserviceKey)) {

                String modelId = partialInfo.getMicroservices().get(microserviceKey).getMicroserviceId();
                MLAlgorithm loadedModel = loadModelFromDisk(modelId);

                ProducerRecord<String,MLAlgorithm> msg = new ProducerRecord<>(
                        "model-updates-topic",
                        modelId,        // Key = microservice’s uniqueModelId
                        loadedModel      // Value = serialized model
                );
                aggregatorProducer.send(msg);

                System.out.println("*** Aggregator published LOAD_MODEL for => " + modelId);


                if (loadedModel == null) {
                    System.err.println("LOAD command: Could not find model file for " + modelId);
                } else {

                    GenericMLAlgorithmMicroservice ms = localMicroMap.get(modelId);
                    if (ms == null) {
                        // The aggregator store says we have membership,
                        // but physically not running => create a new instance
                        ms = new GenericMLAlgorithmMicroservice(
                                command.getAlgorithmType(),
                                command.getAlgorithmID(),
                                aggregatorKey,
                                command.getTarget(),
                                command.getTaskType(),
                                modelId,
                                command.getHyperParams()
                        );
                        ms.clear();
                        ms.start();
                        localMicroMap.put(modelId, ms);
                        System.out.println("*** Re-created local microservice => " + modelId);
                    }
                    else {
                     System.out.println("Microservice Does not exist in this instance " + localMicroMap.containsKey(modelId));
                    }


                }
                changed = true;
            }
        }


        if (changed) {
            store.put(aggregatorKey, partialInfo);

            // Forward the partialRecord => aggregator merges it
            Record<String, MicroServiceInfo> outRecord = new Record<>(
                    aggregatorKey,
                    partialRecord,
                    record.timestamp()
            );
            context.forward(outRecord);
        }
    }

    @Override
    public void close() {
        // Nothing special
    }



    private String buildAggregatorKey(ControlStructure command) {
        String sId = command.getStreamID();
        String dKey = command.getDataSetKey();
        if (sId != null && !sId.isEmpty()) {
            return sId + "-" + dKey;
        } else {
            return dKey;
        }
    }

/*
    private String buildMicroserviceKey(ControlStructure command) {
        // For instance: "algorithmType#target" or "algorithmType#target#taskType", up to you
        return command.getAlgorithmType() + "#" + command.getTarget();
    }

 */

    private String buildMicroserviceKey(ControlStructure command) {
        // e.g. "algorithmType#target#<hash_of_params>"
        String paramSig = paramSignature(command.getHyperParams());
        return command.getAlgorithmType() + "#" + command.getTarget() + "#" + paramSig;
    }

    private String paramSignature(Map<String,Object> hyperParams) {
        if (hyperParams == null || hyperParams.isEmpty()) {
            return "defaultParams";
        }
        // Convert to a stable string or JSON and hash
        return hyperParams.toString().hashCode() + "";
    }




    private MLAlgorithm loadModelFromDisk(String modelId) {
       // String path = "C:/dataset/Stored/" + modelId + ".bin";

        String subpath = EnvironmentConfiguration.getFilePathPrefix();
        String path = subpath + modelId + ".bin"; // Example path

    //    String basePath = System.getenv().getOrDefault("MODEL_STORAGE_PATH", "/models/");
       // String path = basePath + modelId + ".bin";
        try (FileInputStream fis = new FileInputStream(path);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            MLAlgorithm model = (MLAlgorithm) ois.readObject();
            System.out.println("Loaded model from disk: " + path);
            return model;
        } catch (Exception e) {
            System.err.println("Failed to load model from disk: " + e.getMessage());
            return null;
        }
    }



    private void createTopicsIfNotExists(String aggregatorKey) {


        String dataTopicName = "DataTopic-" + aggregatorKey;
        String predTopicName = "PredTopic-" + aggregatorKey;

        // For example, create data/pred topics for aggregatorKey
      //  String dataTopicName = "DataTopic-" + aggregatorKey;
       // String predTopicName = "PredTopic-" + aggregatorKey;

        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> topics = new ArrayList<>();
            if (!existing.contains(dataTopicName)) {
                topics.add(new NewTopic(dataTopicName, 1, (short) EnvironmentConfiguration.giveTheReplicationFactor()));
            }
            if (!existing.contains(predTopicName)) {
                topics.add(new NewTopic(predTopicName, 1, (short) EnvironmentConfiguration.giveTheReplicationFactor()));
            }
            if (!topics.isEmpty()) {
                admin.createTopics(topics).all().get();
                System.out.printf("Created topics  => %s, %s%n", dataTopicName, predTopicName);
            }
        } catch (ExecutionException ee) {
            if (ee.getCause() != null
                    && ee.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
                System.out.println(" Algorithms Topics already exist, ignoring.");
            } else {
                throw new RuntimeException(ee);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Properties adminProps() {
        Properties properties = new Properties();
     //   p.put("bootstrap.servers", "broker:9092");
        properties.put( AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return properties;
    }


    private Properties getProducerProps(){

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MLAlgorithmSerializer.class.getName());

        return  props;
    }
}
