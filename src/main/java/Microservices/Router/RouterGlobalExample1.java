package Microservices.Router;


import Microservices.Ensemble.EnsembleAggregatorMicroservice;
//import Microservices.Processors.AggregatorProcessor;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Illustrates:
 *  - Round-robin "control-topic" => aggregator => produce to "active-microservices"
 *  - GlobalKTable from "active-microservices"
 *  - KStream from "training-topic" => join with GlobalKTable => produce to microservice topics
 */
public class RouterGlobalExample1 {

    private final ConcurrentHashMap<String, EnsembleAggregatorMicroservice> aggregatorMap;
    private EnsembleAggregatorMicroservice aggregator;

    private final KafkaStreams streams;
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

    AtomicInteger counter = new AtomicInteger(0);

    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private long startTime;
    private long lastLogTime;

    public RouterGlobalExample1() {
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = this.startTime;

        this.latch =new CountDownLatch(1);

        aggregatorMap = new ConcurrentHashMap<>();

        CreateTopic createTopic = new CreateTopic();

        //  TopicCreation.createTopicsForStreamIdIfNotExists


        // SerDes
        controlSerde = new ControlStructureSerde();
        microserviceInfoSerde = new GeneralSerde<>(MicroServiceInfo.class);
        dataSerde = new DataStructureSerde();

        StreamsBuilder builder = new StreamsBuilder();

/*
        GlobalKTable<String, MicroServiceInfo> globalActiveTable = builder.globalTable(
                ACTIVE_MICROSERVICES_TOPIC,
                Consumed.with(Serdes.String(), microserviceInfoSerde),
                Materialized.as("GlobalMicroserviceStore")
        );

 */



        // 1) Build aggregator store (for aggregator transformer)
        StoreBuilder<KeyValueStore<String, MicroServiceInfo>> aggregatorStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(AGGREGATOR_STORE),
                        Serdes.String(),
                        microserviceInfoSerde
                );
        builder.addStateStore(aggregatorStoreBuilder);


/*
        // 2) KStream of control commands => aggregator => produce to "active-microservices"
        KStream<String, ControlStructure> controlStream = builder.stream(
                CONTROL_TOPIC,
                Consumed.with(Serdes.String(), controlSerde)
        ).repartition(Repartitioned.<String, ControlStructure>as( "ControlRoundRobinRepartitioned")
                .withKeySerde(Serdes.String())
                .withValueSerde(controlSerde)
                .withStreamPartitioner(new CustomStreamPartitioner5()));


        KStream<String, MicroServiceInfo> aggregatorUpdates = controlStream
                .transform(
                        () -> new AggregatorTransformer2(AGGREGATOR_STORE),
                        AGGREGATOR_STORE
                );

 */

        KStream<String, ControlStructure> originalControl = builder.stream(
                CONTROL_TOPIC,
                Consumed.with(Serdes.String(), controlSerde)
        );

        KStream<String, ControlStructure> broadcasted = originalControl
                .selectKey((oldKey, record) -> record.getStreamID())
                .flatMap((key, command) -> {
                    List<KeyValue<String, ControlStructure>> results = new ArrayList<>();
                    if ("DELETE".equalsIgnoreCase(command.getCommandType())) {
                        for (int p = 0; p < 4; p++) {
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
       //KStream<String, MicroServiceInfo> aggregatorUpdates = controlStream
             //   .process(() -> new AggregatorProcessor(AGGREGATOR_STORE), AGGREGATOR_STORE);



        //aggregatorUpdates.to(PARTIAL_MICROSERVICES_TOPIC,
           //     Produced.with(Serdes.String(), microserviceInfoSerde));


        KStream<String, MicroServiceInfo> partialStream = builder.stream(
                PARTIAL_MICROSERVICES_TOPIC,
               Consumed.with(Serdes.String(),microserviceInfoSerde));





        KTable<String, MicroServiceInfo> algorithmMicroserviceTable = partialStream
                .groupByKey(Grouped.with(Serdes.String(), microserviceInfoSerde))
                .aggregate(
                        () -> new MicroServiceInfo(),
                        (streamId, partialInfo, FullState) -> {
                            // 1) unify partialInfo -> oldFull
                            if (FullState.getStreamId() == null) {
                                FullState.setStreamId(streamId);
                                // createTopicsForAlgorithmInstance(streamId,1,1);
                            }
                            for (Map.Entry<String, MicroserviceDetails> e : partialInfo.getMicroservices().entrySet()) {
                                String microserviceKey = e.getKey();
                                MicroserviceDetails details = e.getValue();

                                if (details.isRemoved()) {
                                    // remove from fullState if present
                                    FullState.removeMicroservice(microserviceKey);
                                } else {
                                    // add if not present
                                    if (!FullState.getMicroservices().containsKey(microserviceKey)) {
                                      //  FullState.addMicroservice(
                                            //    microserviceKey,
                                              //  details.getAlgorithmType(),
                                              //  details.getAlgorithmId(),
                                               // details.getTarget(),details.getTaskType()
                                     //   );
                                    }
                                }
                            }


                            // 2) Now oldFull is the "full aggregator" for this streamID
                            // => we do the "≥3 microservices?" check
                            /*
                            long count = oldFull.getMicroservices().size();
                            if (!aggregatorMap.containsKey(streamId) && count >= 2) {
                                System.out.printf("Starting ensemble aggregator => streamId=%s, count=%d\n",
                                        streamId, count);

                                EnsembleAggregatorMicroservice aggregator = new EnsembleAggregatorMicroservice(streamId, count);
                                aggregator.cleanUp();
                                aggregator.start();
                                aggregatorMap.put(streamId, aggregator);
                            }

                            return oldFull; // returning the final aggregator

                             */
/*
                            ArrayList<String> aggregatorKeys = new ArrayList<>();

                            for(int i=0 ;i< FullState.getMicroservices().size();i++) {
                                aggregatorKeys.add(streamId + FullState.getMicroservices().get(i).getTarget());
                                System.out.println("KEYS:"+  aggregatorKeys.add(streamId + FullState.getMicroservices().get(i).getTarget()));

                            }

 */


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
                                String aggregatorKey = streamId + "#" + target;
                                String taskType = microservicesForTarget.get(0).getTaskType();

                                EnsembleAggregatorMicroservice aggregatorForTarget = aggregatorMap.get(aggregatorKey);

                                // If we have at least 2 microservices => start aggregator (or update it)
                                if (countForTarget >= 2) {
                                    if (aggregatorForTarget == null) {
                                        aggregatorForTarget = new EnsembleAggregatorMicroservice(streamId,target,taskType, countForTarget);
                                        aggregatorForTarget.cleanUp();
                                        aggregatorForTarget.start();
                                        aggregatorMap.put(aggregatorKey, aggregatorForTarget);
                                        System.out.printf("Started aggregator for %s (target=%s) with count=%d\n",
                                                streamId, target, countForTarget);

                                        createTopic.createTopicsForStreamIdAndTargetIfNotExists(OUTPUT_TOPIC,streamId,target);
                                    } else {
                                        aggregatorForTarget.updateCount(countForTarget);
                                        System.out.printf("Updated aggregator membership => streamId=%s, target=%s, count=%d\n",
                                                streamId, target, countForTarget);
                                    }
                                } else {
                                    // If membership < 2 => stop aggregator if running
                                    if (aggregatorForTarget != null) {
                                        aggregatorForTarget.stop();
                                        aggregatorForTarget.cleanUp();
                                        aggregatorMap.remove(aggregatorKey);
                                        System.out.printf("Stopped aggregator for %s => target=%s => count=%d\n",
                                                streamId, target, countForTarget);
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

/*
        KStream<String ,MicroServiceInfo> finalStream = builder.stream(
                ACTIVE_MICROSERVICES_TOPIC,
                Consumed.with(Serdes.String(),microserviceInfoSerde)
        );

 */



/*
        KStream<String, MicroServiceInfo> EnsembleStream =  builder.stream(ACTIVE_MICROSERVICES_TOPIC,Consumed.with(Serdes.String(), microserviceInfoSerde));

        EnsembleStream.foreach((key, fullInfo) -> {
            long count = fullInfo.getMicroservices().size();
            if (!aggregatorMap.containsKey(fullInfo.getStreamId()) && count >=3) {
                System.out.printf("Starting ensemble aggregator for streamId=%s, count=%d%n",
                        fullInfo.getStreamId(), count);
                //  EnsembleAggregatorMicroservice aggregator =
                aggregator= new EnsembleAggregatorMicroservice(fullInfo.getStreamId(), count);

                aggregator.cleanUp(); // optional
                aggregator.start();

                aggregatorMap.put(fullInfo.getStreamId(), aggregator);
            }
            else {
                System.out.printf("No ensemble aggregator started for %s => count=%d%n",
                        fullInfo.getStreamId(), count);
            }
        });

 */



        // 4) KStream of training data => join with global table => produce to microservice topics
        processDataStream(builder, TRAINING_TOPIC, globalActiveTable, dataSerde, false);

        // 5) Possibly the same for PREDICTION_TOPIC
        processDataStream(builder, PREDICTION_TOPIC, globalActiveTable, dataSerde, true);



        this.streams = new KafkaStreams(builder.build(), getStreamsConfig());

        //print the topology
        System.out.println("TOPOLOGY##:" +builder.build().describe());
    }




    private void processDataStream(
            StreamsBuilder builder,
            String inputTopic,
            GlobalKTable<String, MicroServiceInfo> globalActiveTable,
            Serde<DataStructure> dataSerde,
            boolean isPrediction
    ) {
        // 1) Build the input KStream
        KStream<String, DataStructure> dataStream = builder.stream(
                        inputTopic,
                        Consumed.with(Serdes.String(), dataSerde)
                )
                // Key each data record by "streamID" (or whatever you want)
                .selectKey((oldKey, record) -> record.getStreamID())
                // Repartition for parallelism
                .repartition(
                        Repartitioned.<String, DataStructure>as(inputTopic + "-roundRobinRepartitioned")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(dataSerde)
                                .withStreamPartitioner(new CustomStreamPartitioner4())
                );

        // 2) Join with global table, producing just ONE TopicDataWrapper or null
        KStream<String, TopicDataWrapper> joined =
                dataStream.join(
                        globalActiveTable,
                        /* KeyValueMapper => map data's key to the global table key */
                        (dataKey, dataVal) -> dataKey,
                        /* ValueJoiner => returns a single TopicDataWrapper or null */
                        (dataVal, microserviceInfo) -> {
                            if (microserviceInfo == null) {
                                // No known membership => no output
                                return null;
                            }
                            String topicSuffix = isPrediction
                                    ? "PredTopicMAlgorithm-"
                                    : "DataTopicMAlgorithm-";
                            // e.g. "DataTopicMAlgorithm-<streamId>" or "PredTopicMAlgorithm-<streamId>"
                            String outputTopic = topicSuffix + microserviceInfo.getStreamId();

                            // Build a single wrapper
                            return new TopicDataWrapper(outputTopic, dataVal);
                        }
                );

        // 3) Filter out null results => produce to the dynamic topic
        joined
               // .filter((k, wrapper) -> wrapper != null)
                .peek((k, v) -> recordProcessed()) // measure throughput if you like
                .to(
                        // `TopicNameExtractor` to pick the topic name from wrapper
                        (key, wrapper, recordContext) -> wrapper.getTopicName(),
                        Produced.with(Serdes.String(), new TopicDataWrapperSerde())
                );
    }




    private Properties getStreamsConfig() {
        Properties props = new Properties();


        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,4);  // Match partition count

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "RouterMicroservice1");

        props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/dataset/tmp/kafka-streams");
      //  props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams" );

        // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Add cache configuration for better performance
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L); // 10MB cache

        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);

        // props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536);

        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);

        // props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "2097152"); // 2MB


        props.put("compression.type", "snappy");
        // props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);

        // props.put("partition.assignment.strategy",
        //  "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        return props;
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


        RouterGlobalExample router = new RouterGlobalExample();
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
            router.start();
            router.latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }
}
