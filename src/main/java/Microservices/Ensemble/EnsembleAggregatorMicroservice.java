package Microservices.Ensemble;

import Configuration.EnvironmentConfiguration;
import Evaluation.ClassificationEvaluationService;
import Evaluation.RegressionEvaluationService;
import Microservices.Router.MicroServiceInfo;
import Serdes.Init.ListOfPartialPredictionSerde;
import Structure.EnsembleResult;
import Structure.PartialPrediction;
import Serdes.GeneralFormat.GeneralSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A standalone microservice that aggregates partial predictions into a final EnsembleResult.
 * It is state-aware and reacts to changes in the set of active algorithms for its group.
 */
public class EnsembleAggregatorMicroservice {
    private final String streamId;
    private final String target;
    private final String taskType;

    private KafkaStreams streams;
    private final AtomicLong totalRecordsProcessed;
    private long startTime;
    private long lastLogTime;

    /**
     * Constructor for the state-aware ensemble aggregator.
     * @param streamId The data scope key (e.g., "AegeanShips-Ships").
     * @param target The target variable for this ensemble (e.g., "status").
     * @param taskType The type of ML task ("Classification" or "Regression").
     */
    public EnsembleAggregatorMicroservice(String streamId, String target, String taskType) {
        this.streamId = streamId;
        this.target = target;
        this.taskType = taskType;
        this.totalRecordsProcessed = new AtomicLong(0);
    }

    /**
     * Builds and starts the Kafka Streams application for ensemble aggregation.
     */
    public void start() {
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = this.startTime;

        StreamsBuilder builder = new StreamsBuilder();

        // --- DEFINE TOPICS ---
        String ensembleInputTopic = "EnsembleTopicForData-" + streamId + "-" + target;
        String outputTopic = "OutputTopicForData-" + streamId + "-" + target;
        String activeMicroservicesTopic = "active-microservices";

        // --- DEFINE STATE STORE NAMES ---
        String membershipStateStoreName = "membership-state-store-for-" + streamId + "-" + target;
        String partialsStoreName = "partials-agg-store-for-" + streamId + "-" + target;

        // --- DEFINE SERDES ---
        Serde<String> stringSerde = Serdes.String();
        Serde<PartialPrediction> partialPredictionSerde = new GeneralSerde<>(PartialPrediction.class);
        Serde<EnsembleResult> ensembleResultSerde = new GeneralSerde<>(EnsembleResult.class);
        Serde<MicroServiceInfo> microserviceInfoSerde = new GeneralSerde<>(MicroServiceInfo.class);
        Serde<List<PartialPrediction>> listOfPredictionsSerde = new ListOfPartialPredictionSerde();

        // --- CREATE THE MEMBERSHIP KTABLE ---
        // This KTable is the "source of truth" for which algorithms are currently active for any group.
        builder.table(
                activeMicroservicesTopic,
                Consumed.with(stringSerde, microserviceInfoSerde),
                Materialized.<String, MicroServiceInfo, KeyValueStore<Bytes, byte[]>>as(membershipStateStoreName)
                        .withKeySerde(stringSerde)
                        .withValueSerde(microserviceInfoSerde)
        );

        // --- CREATE THE PARTIAL RESULTS STORE ---
        // This store holds the incoming predictions for each recordID.
        StoreBuilder<KeyValueStore<String, List<PartialPrediction>>> partialsStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(partialsStoreName),
                        stringSerde,
                        listOfPredictionsSerde
                );
        builder.addStateStore(partialsStoreBuilder);

        // --- DEFINE THE INPUT STREAM OF PARTIAL PREDICTIONS ---
        KStream<String, PartialPrediction> partialStream = builder.stream(
                ensembleInputTopic,
                Consumed.with(stringSerde, partialPredictionSerde)
        );

        // --- START EVALUATION SERVICES (if applicable) ---
        if ("Classification".equalsIgnoreCase(taskType)) {
            ClassificationEvaluationService eval = new ClassificationEvaluationService(streamId, target);
            eval.cleanUp();
            eval.start();
        } else {
            RegressionEvaluationService eval = new RegressionEvaluationService(streamId, target);
            eval.cleanUp();
            eval.start();
        }

        // --- PROCESS THE STREAM ---
        KStream<String, EnsembleResult> finalEnsembleStream = partialStream.process(
                () -> new EnsembleAggregatorProcessor(
                        partialsStoreName,
                        membershipStateStoreName,
                        this.streamId, // Pass the correct lookup key (the streamId)
                        this.target    // Pass the target for filtering
                ),
                // You must explicitly connect BOTH state stores that the processor needs to access.
                partialsStoreName,
                membershipStateStoreName
        );

        // --- WRITE FINAL RESULTS TO OUTPUT TOPIC ---
        finalEnsembleStream.peek((k, v) -> recordProcessed())
                .to(outputTopic, Produced.with(stringSerde, ensembleResultSerde));

        // Build and start the Kafka Streams application
        this.streams = new KafkaStreams(builder.build(), getStreamsConfig());
        this.streams.start();
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

    public void cleanUp() {
        if (streams != null) {
            streams.cleanUp();
        }
    }

    private Properties getStreamsConfig() {
        Properties props = new Properties();
        // Must be unique for each aggregator instance to avoid state conflicts
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EnsembleAggregatorMicroserviceFor-" + streamId + "-" + target);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EnvironmentConfiguration.getBootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, EnvironmentConfiguration.getTempDir());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Increase cache for better performance with stateful operations
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L); // 10MB
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // Commit every second

        return props;
    }

    private void recordProcessed() {
        long currentCount = totalRecordsProcessed.incrementAndGet();
        if (currentCount % 10000 == 0) {
            long currentTime = System.currentTimeMillis();
            double totalElapsedSeconds = (currentTime - startTime) / 1000.0;
            double totalThroughput = currentCount / totalElapsedSeconds;

            double deltaSeconds = (currentTime - lastLogTime) / 1000.0;
            // Avoid division by zero if deltaSeconds is too small
            if (deltaSeconds > 0) {
                double instantThroughput = 10000.0 / deltaSeconds;
                System.out.printf("Instant Throughput EXIT (last 10k records): %.2f records/second%n", instantThroughput);
            }

            System.out.printf(
                    "Throughput EXIT: %.2f records/second (Processed: %d records in %.2f seconds)%n",
                    totalThroughput, currentCount, totalElapsedSeconds
            );

            lastLogTime = currentTime;
        }
    }
}
