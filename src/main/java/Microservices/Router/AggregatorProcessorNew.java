package Microservices.Router;

import Configuration.EnvironmentConfiguration;
import Microservices.MLAlgorithmMicroService.GenericMLAlgorithmMicroservice;
import Serdes.MLAlgorithmSerializer;
import Structure.ControlStructure;
import mlAlgorithms.MLAlgorithm;
import helperClasses.CreateTopic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
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
 * Approach B aggregator => each store record is 1 microservice.
 * The final partition key is aggregatorBase + "___" + microserviceBase.
 * aggregatorBase = (streamId-datasetKey) or datasetKey alone
 * microserviceBase = algorithmType#target#paramSignature
 * => storeKey = exactly that partition key.
 *
 * We parse aggregatorBase so we can create the data/pred topics "DataTopic-..." or "PredTopic-..."
 */
public class AggregatorProcessorNew implements Processor<String, ControlStructure, String, MicroServiceInfo> {
    private static final ConcurrentHashMap<String, GenericMLAlgorithmMicroservice> localMicroMap = new ConcurrentHashMap<>();
    private static final String DEFAULT_MODEL_PATH = "/models/";

    private final String storeName;
    private ProcessorContext<String, MicroServiceInfo> context;
    private KeyValueStore<String, MicroServiceInfo> store;
    private final CreateTopic createTopic = new CreateTopic();

    public AggregatorProcessorNew(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, MicroServiceInfo> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, ControlStructure> record) {
        ControlStructure cmd = record.value();
        // The key for the aggregator store is the same as the partition key => record.key()
        // Because we want 1 store record = 1 microservice
        String storeKey = record.key();  // e.g. "SHIPS1-Forex___DecisionTrees#status#someHash"
        if (storeKey == null) return; // ignore

        // If not exist => create a brand new MicroServiceInfo for this single microservice
        MicroServiceInfo membership = store.get(storeKey);
        if (membership == null) {
            membership = new MicroServiceInfo();
            // parse aggregatorBase => to create data/pred topics
            String aggregatorBase = parseAggregatorBase(storeKey);
            membership.setStreamId( parseStreamId(aggregatorBase) );
            membership.setDatasetKey(parseDatasetKey(aggregatorBase) );
            createTopicsIfNotExists(aggregatorBase);
        }
        String aggregatorBase = parseAggregatorBase(storeKey);
        MicroServiceInfo partialRecord = new MicroServiceInfo();
        partialRecord.setStreamId(parseStreamId(aggregatorBase));
        partialRecord.setDatasetKey(parseDatasetKey(aggregatorBase));

        boolean changed = false;

        if ("CREATE".equalsIgnoreCase(cmd.getCommandType())) {
            // For a single-record MicroServiceInfo, let's define the microserviceKey as "unique"
            // In Approach B, we typically do membership.getMicroservices() with only 1 entry
            String microKey = buildMicroserviceKey(cmd);
            if (!membership.hasMicroserviceKey(microKey)) {
                membership.addMicroservice(
                        microKey,
                        cmd.getAlgorithmType(),
                        cmd.getAlgorithmID(),
                        cmd.getTarget(),
                        cmd.getTaskType(),
                        cmd.getHyperParams()
                );
                changed = true;

                // physically start the local microservice
                MicroserviceDetails details = membership.getMicroservices().get(microKey);
                String microserviceId = details.getMicroserviceId();
                if (!localMicroMap.containsKey(microserviceId)) {
                    GenericMLAlgorithmMicroservice ms = new GenericMLAlgorithmMicroservice(
                            cmd.getAlgorithmType(),
                            cmd.getAlgorithmID(),
                            parseAggregatorBase(storeKey), // aggregatorBase
                            cmd.getTarget(),
                            cmd.getTaskType(),
                            microserviceId,
                            cmd.getHyperParams()
                    );
                    ms.clear();
                    ms.start();
                    localMicroMap.put(microserviceId, ms);
                    System.out.printf("*** Created microservice => storeKey=%s, microKey=%s%n",
                            storeKey, microKey);

                    String TopicName = "EnsembleTopicForData";

                    createTopic.createTopicsForStreamIdAndTargetIfNotExists(TopicName, parseAggregatorBase(storeKey), cmd.getTarget());
                }
                 details.setRemoved(false);
                partialRecord.getMicroservices().put(microKey, details);
            }
        }
        else if ("DELETE".equalsIgnoreCase(cmd.getCommandType())) {
            String microKey = buildMicroserviceKey(cmd);
            if (membership.hasMicroserviceKey(microKey)) {
                MicroserviceDetails existing = membership.getMicroservices().get(microKey);
                String microserviceId = existing.getMicroserviceId();

                // physically stop
                GenericMLAlgorithmMicroservice ms = localMicroMap.remove(microserviceId);
                if (ms != null) {
                    ms.stop();
                    ms.clear();
                    System.out.printf("*** Stopped microservice => storeKey=%s, microKey=%s%n",
                            storeKey, microKey);
                }
                membership.removeMicroservice(microKey);

                changed = true;

                existing.setRemoved(true);
                partialRecord.getMicroservices().put(microKey, existing);
            }
        }
        else if ("LOAD".equalsIgnoreCase(cmd.getCommandType())) {
            String microKey = buildMicroserviceKey(cmd);
            if (membership.hasMicroserviceKey(microKey)) {
                MicroserviceDetails existing = membership.getMicroservices().get(microKey);
                String modelId = existing.getMicroserviceId();

                // load model from disk
                MLAlgorithm loadedModel = loadModelFromDisk(modelId);
                if (loadedModel == null) {
                    System.err.println("LOAD => model not found on disk => " + modelId);
                } else {
                    // produce to "model-updates-topic"
                    KafkaProducer<String, MLAlgorithm> aggregatorProducer = new KafkaProducer<>(getProducerProps());
                    aggregatorProducer.send(
                            new ProducerRecord<>("model-updates-topic", modelId, loadedModel)
                    );
                    aggregatorProducer.close();

                    System.out.println("*** Aggregator published LOAD_MODEL for => " + modelId);

                    GenericMLAlgorithmMicroservice ms = localMicroMap.get(modelId);
                    if (ms == null) {
                        // create new instance physically
                        ms = new GenericMLAlgorithmMicroservice(
                                cmd.getAlgorithmType(),
                                cmd.getAlgorithmID(),
                                parseAggregatorBase(storeKey),
                                cmd.getTarget(),
                                cmd.getTaskType(),
                                modelId,
                                cmd.getHyperParams()
                        );
                        ms.clear();
                        ms.start();
                        localMicroMap.put(modelId, ms);
                        System.out.println("*** Re-created microservice => " + modelId);
                    }

                    else {
                        System.out.println("Microservice  already exists in this instance " + localMicroMap.containsKey(modelId));
                    }
                }
                changed = true;
            }
        }

        if (changed) {
            store.put(storeKey, membership);
            // Forward partial membership => aggregator merges if you want
            Record<String, MicroServiceInfo> outRecord = new Record<>(
                    storeKey, partialRecord, record.timestamp()
            );
            context.forward(outRecord);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    // aggregatorBase => parse out the portion up to "___"
    private String parseAggregatorBase(String storeKey) {
        int idx = storeKey.indexOf("___");
        if (idx < 0) return storeKey;  // no delimiter => entire storeKey is aggregatorBase
        return storeKey.substring(0, idx);
    }

    private String parseStreamId(String aggregatorBase) {
        // aggregatorBase is either "streamId-datasetKey" or just "datasetKey"
        // if there's a '-', we interpret it as "streamId-dKey"
        // If no '-', we interpret aggregatorBase as just datasetKey => streamId = ""
        int dash = aggregatorBase.indexOf("-");
        if (dash < 0) {
            return ""; // no streamId
        } else {
            return aggregatorBase.substring(0, dash);
        }
    }
    private String parseDatasetKey(String aggregatorBase) {
        int dash = aggregatorBase.indexOf("-");
        if (dash < 0) {
            return aggregatorBase;
        } else {
            return aggregatorBase.substring(dash + 1);
        }
    }

    private String buildMicroserviceKey(ControlStructure cmd) {
        String paramSig = paramSignature(cmd.getHyperParams());
        return cmd.getAlgorithmType() + "#" + cmd.getTarget() + "#" + paramSig;
    }

    private String paramSignature(Map<String,Object> hyperParams) {
        if (hyperParams == null || hyperParams.isEmpty()) {
            return "defaultParams";
        }
        return hyperParams.toString().hashCode() + "";
    }

    private MLAlgorithm loadModelFromDisk(String modelId) {
        String basePath = System.getenv().getOrDefault("MODEL_STORAGE_PATH", DEFAULT_MODEL_PATH);
        String path = basePath + modelId + ".bin";
        try (FileInputStream fis = new FileInputStream(path);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            MLAlgorithm model = (MLAlgorithm) ois.readObject();
            System.out.println("Loaded model from disk: " + path);
            return model;
        } catch (Exception e) {
            System.err.println("Failed to load model => " + e.getMessage());
            return null;
        }
    }

    private void createTopicsIfNotExists(String aggregatorBase) {
        // aggregatorBase => parse streamId, dataSetKey
        String streamId = parseStreamId(aggregatorBase);
        String dKey     = parseDatasetKey(aggregatorBase);

        String dataTopicName;
        String predTopicName;

        if (streamId.isEmpty()) {
            // aggregatorBase is datasetKey only => dataTopicName = "DataTopic-dKey"
            dataTopicName = "DataTopic-" + dKey;
            predTopicName = "PredTopic-" + dKey;
        } else {
            dataTopicName = "DataTopic-"+ streamId + "-" + dKey;
            predTopicName = "PredTopic-" + streamId + "-" + dKey;
        }

        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> topics = new ArrayList<>();
            if (!existing.contains(dataTopicName)) {
                topics.add(new NewTopic(dataTopicName, 1, (short)EnvironmentConfiguration.giveTheReplicationFactor()));
            }
            if (!existing.contains(predTopicName)) {
                topics.add(new NewTopic(predTopicName, 1, (short)EnvironmentConfiguration.giveTheReplicationFactor()));
            }
            if (!topics.isEmpty()) {
                admin.createTopics(topics).all().get();
                System.out.printf("Created data/pred topics => %s, %s%n", dataTopicName, predTopicName);
            }
        } catch (ExecutionException ee) {
            if (ee.getCause() != null
                    && ee.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
                System.out.println("Topics exist, ignoring => " + aggregatorBase);
            } else {
                throw new RuntimeException(ee);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, EnvironmentConfiguration.getBootstrapServers());
        return props;
    }
    private Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvironmentConfiguration.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MLAlgorithmSerializer.class.getName());
        return props;
    }
}
