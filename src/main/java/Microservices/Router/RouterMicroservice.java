package Microservices.Router;


import Configuration.CreateConfiguration;
import Configuration.EnvironmentConfiguration;
import Microservices.Ensemble.EnsembleAggregatorMicroservice;
//import Microservices.Processors.AggregatorProcessor;
//import Microservices.Processors.AggregatorTransformerDelete;
import Structure.ControlStructure;
import Structure.DataStructure;
import helperClasses.*;
import Serdes.Init.ControlStructureSerde;
import Serdes.Init.DataStructureSerde;
import Serdes.Init.TopicDataWrapperSerde;
import Serdes.GeneralFormat.GeneralSerde;
import helperClasses.CreateTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;



import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Illustrates:
 *  - Round-robin "control-topic" => aggregator
 *  - Group per streamID-datasetKey into a Ktable - starting the corresponding ensemble aggregator.
 *  - GlobalKTable from "active-microservices"
 *  - KStream from "training-topic" - "prediction-topic " (same pattern) => join with GlobalKTable => produce to microservice topics
 */
public class RouterMicroservice {

    private final ConcurrentHashMap<String, EnsembleAggregatorMicroservice> aggregatorMap;
    private EnsembleAggregatorMicroservice aggregator;
    private static final String MICROSERVICE_ID = "RouterMicroservice";
    private final  static int replicateFactor = EnvironmentConfiguration.giveTheReplicationFactor();
    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();//"localhost:9092,localhost:9093,localhost:9094";

    final KafkaStreams streams;
    public final CountDownLatch latch ;
    // Where aggregator transformer stores data
    private static final String AGGREGATOR_STORE = "AggregatorStateStore";
    // The topic that aggregator updates get written to
    private static final String ACTIVE_MICROSERVICES_TOPIC = "active-microservices";

    private static final String PARTIAL_MICROSERVICES_TOPIC = "partial-microservices";

    private static final String CONTROL_TOPIC = "control-topic";
    private static final String TRAINING_TOPIC = "training-topic";
    private static final String PREDICTION_TOPIC = "prediction-topic";

    private static final String OUTPUT_TOPIC = "OutputTopicForData";

    Serde<MicroServiceInfo> microserviceInfoSerde;
    Serde<ControlStructure> controlSerde;
    Serde<DataStructure> dataSerde;

    // private  CreateTopic TopicCreation;
    CreateConfiguration createConfiguration;


    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private long startTime;
    private long lastLogTime;

    public RouterMicroservice() {
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = this.startTime;

        this.latch =new CountDownLatch(1);

        this.createConfiguration = new CreateConfiguration();

      //  Properties properties= createConfiguration.getPropertiesForMicroservice(MICROSERVICE_ID, BOOTSTRAP_SERVERS);

        aggregatorMap = new ConcurrentHashMap<>();

        CreateTopic createTopic = new CreateTopic();

        //  TopicCreation.createTopicsForStreamIdIfNotExists


        // SerDes
        controlSerde = new ControlStructureSerde();
        microserviceInfoSerde = new GeneralSerde<>(MicroServiceInfo.class);
        dataSerde = new DataStructureSerde();

        StreamsBuilder builder = new StreamsBuilder();



        // 1) Build aggregator store (for aggregator transformer)
        StoreBuilder<KeyValueStore<String, MicroServiceInfo>> aggregatorStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(AGGREGATOR_STORE),
                        Serdes.String(),
                        microserviceInfoSerde
                );
        builder.addStateStore(aggregatorStoreBuilder);




        KStream<String, ControlStructure> originalControl = builder.stream(
                CONTROL_TOPIC,
                Consumed.with(Serdes.String(), controlSerde)
        );


        /*

        KStream<String, ControlStructure> broadcasted = originalControl
               // .selectKey((oldKey, record) -> record.getStreamID()+ "-" +record.getDataSetKey())
                .selectKey((oldKey, command) -> {
                    String sId = command.getStreamID();
                    String dKey = command.getDataSetKey();
                    if (sId != null && !sId.isEmpty()) {
                        return sId + "-" + dKey;
                    } else {
                        return dKey;
                    }
                })
                .flatMap((key, command) -> {
                    List<KeyValue<String, ControlStructure>> results = new ArrayList<>();
                    if ("DELETE".equalsIgnoreCase(command.getCommandType()) || "LOAD".equalsIgnoreCase(command.getCommandType())) {
                        for (int p = 0; p < EnvironmentConfiguration.giveTheParallelDegree(); p++) {
                            results.add(KeyValue.pair(key , command));
                        }
                    } else {
                        results.add(KeyValue.pair(key, command));
                    }
                    return results;
                });

// (B) Repartition

        KStream<String, ControlStructure> controlStream = broadcasted
                .repartition(Repartitioned.<String, ControlStructure>as("ControlRoundRobinRepartitioned")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(controlSerde)
                        .withStreamPartitioner(new CustomStreamPartitioner5()));




// (C) aggregator transform -> aggregatorUpdates
        KStream<String, MicroServiceInfo> aggregatorUpdates = controlStream
                .process(() -> new ControlProcessorRoundRobin(AGGREGATOR_STORE), AGGREGATOR_STORE);





         */







        KStream<String, ControlStructure> withFinalKey = originalControl.map(
                        (oldKey, cmd) -> {
                            String finalKey = buildPartitionKey(cmd);  // aggregatorBase + "___" + microserviceBase
                            return KeyValue.pair(finalKey, cmd);
                        }
                )
                // Repartition  among partitions, but by finalKey – so the same finalKey => same partition
                .repartition(
                        Repartitioned.<String, ControlStructure>as("ControlRepartitioned")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(controlSerde)
                );

        KStream<String, MicroServiceInfo> aggregatorUpdates = withFinalKey.process(
                () -> new ControlProcessorOnID(AGGREGATOR_STORE),
                AGGREGATOR_STORE
        );







        KTable<String, MicroServiceInfo> algorithmMicroserviceTable = aggregatorUpdates

                .selectKey((oldKey, command) -> {
                   int idx = oldKey.indexOf("___");
                    if (idx < 0) return oldKey;  // no delimiter => entire storeKey is aggregatorBase
                   return oldKey.substring(0, idx);
                })


                .groupByKey(Grouped.with(Serdes.String(), microserviceInfoSerde))
                .aggregate(
                        () -> new MicroServiceInfo(),
                        (aggregatorKey, partialInfo, FullState) -> {
                            // 1) unify partialInfo -> oldFull
                          //  if (FullState.getStreamId() == null) {
                              //  FullState.setStreamId(streamId);
                            if (FullState.getStreamId() == null && FullState.getDatasetKey() == null) {
                                // initialize
                                FullState.setStreamId(partialInfo.getStreamId());
                                FullState.setDatasetKey(partialInfo.getDatasetKey());
                                // createTopicsForAlgorithmInstance(streamId,1,1);
                            }
                            for (Map.Entry<String, MicroserviceDetails> e : partialInfo.getMicroservices().entrySet()) {
                                String microserviceKey = e.getKey();
                                MicroserviceDetails details = e.getValue();
                                System.out.println("Microservice key:" +microserviceKey);
                                 System.out.println("Microservice Details:"+details);

                                if (details.isRemoved() && FullState.getMicroservices().containsKey(microserviceKey) ) {
                                    System.out.println("DELETE ON :"+ microserviceKey );
                                    // remove from fullState if present
                                    FullState.removeMicroservice(microserviceKey);
                                } else {
                                    // add if not present
                                    if (!FullState.getMicroservices().containsKey(microserviceKey)) {
                                        System.out.println("ADDING FOR: "+ aggregatorKey);
                                        FullState.addMicroservice(
                                                microserviceKey,
                                                details.getAlgorithmType(),
                                                details.getAlgorithmId(),
                                                details.getTarget(),details.getTaskType(),details.getHyperParams()
                                        );
                                    }
                                }
                            }

                            if (FullState.getAllMicroservices().isEmpty()) {

                                System.out.println("All microservices removed for aggregatorKey: " + aggregatorKey + ". Deleting entry.");

                                return null;
                            }




/*
                            long count = FullState.getMicroservices().size();
                             aggregator = aggregatorMap.get(streamId);

                            System.out.println("I am here ");

                            if(aggregator != null){
                                System.out.println("Aggregator seems started");
                            }

                            if (count >= 2) {
                                if (aggregator == null) {
                                    aggregator = new EnsembleAggregatorMicroservice(streamId, count);
                                    aggregator.cleanUp();
                                    aggregator.start();
                                    aggregatorMap.put(streamId, aggregator);
                                    System.out.printf("Started aggregator for %s with count=%d\n", streamId, count);

                                    createTopic.createTopicsForStreamIdIfNotExists(streamId,OUTPUT_TOPIC);

                                } else {
                                    // If aggregator already started, you may want to “update membership”
                                    aggregator.updateCount(count);
                                    System.out.printf("Updated aggregator membership => %s has now count=%d\n", streamId, count);
                                }
                            } else {
                                // If membership < 3 => stop aggregator if running
                                if (aggregator != null) {
                                    aggregator.stop();
                                    aggregator.cleanUp();
                                    aggregatorMap.remove(streamId);

                                    // DELETE TOPICS HERE #############

                                    System.out.printf("Stopped aggregator for %s => count=%d\n", streamId, count);
                                }
                            }

 */

                            Map<String, List<MicroserviceDetails>> byTarget = new HashMap<>();
                            for (MicroserviceDetails details : FullState.getAllMicroservices()) {
                                String target = details.getTarget();
                                // String taskType =details.getTaskType();
                                byTarget.computeIfAbsent(target, k -> new ArrayList<>()).add(details);
                            }

                            // 2) For each distinct target
                            for (Map.Entry<String, List<MicroserviceDetails>> entry : byTarget.entrySet()) {
                                String target = entry.getKey();
                                List<MicroserviceDetails> microservicesForTarget = entry.getValue();
                                long countForTarget = microservicesForTarget.size();

                                // Build a sub-key combining (streamId, target)
                                String EnsembleAggregatorKey = aggregatorKey + "#" + target;
                                String taskType = microservicesForTarget.get(0).getTaskType();

                                EnsembleAggregatorMicroservice aggregatorForTarget = aggregatorMap.get(EnsembleAggregatorKey);

                                // If we have at least 2 microservices => start aggregator (or update it)
                                if (countForTarget >= 2) {
                                    if (aggregatorForTarget == null) {
                                        createTopic.createTopicsForStreamIdAndTargetIfNotExists(OUTPUT_TOPIC,aggregatorKey,target);
                                        aggregatorForTarget = new EnsembleAggregatorMicroservice(aggregatorKey,target,taskType);
                                        aggregatorForTarget.cleanUp();
                                        aggregatorForTarget.start();
                                        aggregatorMap.put(EnsembleAggregatorKey, aggregatorForTarget);
                                        System.out.printf("Started aggregator for %s (target=%s) with count=%d\n",
                                                aggregatorKey, target, countForTarget);

                                    } else {
                                        //aggregatorForTarget.updateCount(countForTarget);
                                        System.out.printf("Updated aggregator membership => {StreamID-dataSetKey} = %s, target=%s, count=%d\n",
                                                aggregatorKey, target, countForTarget);
                                    }
                                } else {
                                    // If membership < 2 => stop aggregator if running
                                    if (aggregatorForTarget != null) {
                                      //  aggregatorForTarget.stop();
                                      //  aggregatorForTarget.cleanUp();
                                      //  aggregatorMap.remove(EnsembleAggregatorKey);
                                        System.out.printf("Stopped aggregator for %s => target=%s => count=%d\n",
                                                aggregatorKey, target, countForTarget);

                                        // ### DELETE OUTPUT TOPIC HERE ###
                                    }
                                }
                            }

                            return FullState;
                        },
                        Materialized.<String, MicroServiceInfo, KeyValueStore<Bytes, byte[]>>as("AlgorithmMicroserviceStore")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(microserviceInfoSerde)
                        //  .withCachingDisabled()
                        //  .withLoggingDisabled()
                );


        algorithmMicroserviceTable
                .toStream()
                .to(ACTIVE_MICROSERVICES_TOPIC, Produced.with(Serdes.String(), microserviceInfoSerde));


        GlobalKTable<String, MicroServiceInfo> globalActiveTable = builder.globalTable(
                ACTIVE_MICROSERVICES_TOPIC,
                Consumed.with(Serdes.String(), microserviceInfoSerde),
                Materialized.as("GlobalMicroserviceStore")
        );



        // 4) KStream of training data => join with global table => produce to microservice topics
        processDataStream(builder, TRAINING_TOPIC, globalActiveTable, dataSerde, false);

        // 5) Possibly the same for PREDICTION_TOPIC
        processDataStream(builder, PREDICTION_TOPIC, globalActiveTable, dataSerde, true);



        this.streams = new KafkaStreams(builder.build(), getStreamsConfig());

        //print the topology
        System.out.println("TOPOLOGY##:" +builder.build().describe());
    }

    private String buildAggregatorBase(ControlStructure cmd){

        String aggregatorBase = (cmd.getStreamID() != null && !cmd.getStreamID().isEmpty())
                ? (cmd.getStreamID() + "-" + cmd.getDataSetKey())
                : cmd.getDataSetKey();
        return aggregatorBase;
    }

    private String buildPartitionKey(ControlStructure cmd) {
        // aggregatorBase => either "streamId-datasetKey" or "datasetKey"
        String aggregatorBase = buildAggregatorBase(cmd);

        // microserviceBase => "algorithmType#target#paramSignature"
        String algo = cmd.getAlgorithmType();
        String tgt  = cmd.getTarget();
        String paramSig = paramSignature(cmd.getHyperParams());
        String microserviceBase = algo + "#" + tgt + "#" + paramSig;

        // combine them => aggregatorBase + "___" + microserviceBase
        return aggregatorBase + "___" + microserviceBase;
    }

    private String paramSignature(Map<String,Object> hyperParams) {
        if (hyperParams == null || hyperParams.isEmpty()) {
            return "defaultParams";
        }
        return hyperParams.toString().hashCode() + "";
    }


    private void processDataStream(StreamsBuilder builder, String inputTopic, GlobalKTable<String, MicroServiceInfo> globalActiveTable,
            Serde<DataStructure> dataSerde,
            boolean isPrediction
    ) {
        // Original data source
        KStream<String, DataStructure> dataStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), dataSerde)
        );

        // --- (A) Sub-stream keyed by "streamId#datasetKey" ---
        KStream<String, DataStructure> keyedByStreamAndDataset = dataStream
                .selectKey((ignored, record) -> record.getStreamID() + "-" + record.getDataSetKey())
                .repartition(Repartitioned.<String, DataStructure>as(inputTopic + "-SpecificStreamRepartitioned")
                .withKeySerde(Serdes.String())
                .withValueSerde(new DataStructureSerde())
              //  .withStreamPartitioner(new CustomStreamPartitioner4())
        );

        KStream<String, TopicDataWrapper> joinedStream = keyedByStreamAndDataset.join(
                globalActiveTable,
                (key, value) -> key,
                (dataVal, microserviceInfo) -> {
                    if (microserviceInfo == null) {
                        return null; // no membership => skip
                    }
                   // String aggregatorKey;
                    String sId = microserviceInfo.getStreamId();
                    String dKey = microserviceInfo.getDatasetKey();

                  //  if (sId != null && !sId.isEmpty())
                       String aggregatorKey = sId + "-" + dKey;

                    // You can build a dynamic topic name or do something else:
                    String outTopic = (isPrediction ? "PredTopic-" : "DataTopic-") + aggregatorKey;
                    // measure throughput
                  //  recordProcessed();
                    return new TopicDataWrapper(outTopic, dataVal);
                }
        );

        //joinedStream
              //  .filter((k, v) -> v != null)
               // .to((k, w, ctx) -> w.getTopicName(),
                      //  Produced.with(Serdes.String(), new TopicDataWrapperSerde()));

        // --- (B) Sub-stream keyed by "datasetKey" only ---
        KStream<String, DataStructure> keyedByDataset = dataStream
                .selectKey((ignored, record) -> record.getDataSetKey())
                .repartition(Repartitioned.<String, DataStructure>as(inputTopic + "-AllDatasetRepartitioned")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new DataStructureSerde())
                        .withStreamPartitioner(new CustomStreamPartitioner4())
                );

        KStream<String, TopicDataWrapper> joinedDataset = keyedByDataset.join(
                globalActiveTable,
                (key, value) -> key,
                (dataVal, microserviceInfo) -> {
                    if (microserviceInfo == null) {
                        return null;
                    }
                    String aggregatorKey = microserviceInfo.getDatasetKey();
                    String outTopic = (isPrediction ? "PredTopic-" : "DataTopic-") + aggregatorKey;
                   // recordProcessed();
                    return new TopicDataWrapper(outTopic, dataVal);
                }
        );


        KStream<String, TopicDataWrapper> mergedJoined = joinedStream.merge(joinedDataset);


        mergedJoined
                .peek((k, v) -> recordProcessed())
                //.filter((k, v) -> v != null)
                .to((k, w, ctx) -> w.getTopicName(),
                        Produced.with(Serdes.String(), new TopicDataWrapperSerde()));
    }


    private Properties getStreamsConfig() {
      //  Properties props = new Properties();

     Properties properties = createConfiguration.getPropertiesForMicroservice(MICROSERVICE_ID, BOOTSTRAP_SERVERS);

        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,EnvironmentConfiguration.giveTheDivideParallelDegree());  // Match partition count
       // properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "RouterMicroservice");

      //  properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/dataset/tmp/kafka-streams");
       // properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams" );

        //properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
         //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");
        //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Add cache configuration for better performance
       // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L); // 10MB cache

        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);

        // props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536);

        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);

        // props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "2097152"); // 2MB

        properties.put("compression.type", "snappy");
        // props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);

      //  properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // props.put("partition.assignment.strategy",
        //  "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        return properties;
    }

    /**
     * Just counting how many records we've processed
     */
    private void recordProcessed() {
        long currentCount = totalRecordsProcessed.incrementAndGet();
        if (currentCount % 10000 == 0) {
            long currentTime = System.currentTimeMillis();
            double totalElapsedSeconds = (currentTime - startTime) / 1000.0;
            double totalThroughput = currentCount / totalElapsedSeconds;

            double deltaSeconds = (currentTime - lastLogTime) / 1000.0;
            double instantThroughput = 10000.0 / deltaSeconds;

            System.out.printf(
                    "Throughput Router: %.2f records/second (Processed: %d records in %.2f seconds) with %d threads%n",
                    totalThroughput, currentCount, totalElapsedSeconds,4

            );

            System.out.printf("Instant Throughput (last 10k records): %.2f records/second%n",
                    instantThroughput);

            lastLogTime = currentTime; // Update lastLogTime to current time
        }
    }

    public void start() {
        streams.start();
    }

    public void clear() {
        streams.cleanUp();
    }





    public void stop() {

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));  // Wait up to 5 seconds for all processing to complete
                clear();
            }
        });
    }


    // public void stop() {
    // streams.close();
    //  }

    public static void main(String[] args) {


        RouterMicroservice router = new RouterMicroservice();
        router.clear();

        // attach shutdown handler to catch SIGINT
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                router.streams.close();
                router.latch.countDown();
            }
        });

        try {
            router.streams.start();
            router.latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }
}
