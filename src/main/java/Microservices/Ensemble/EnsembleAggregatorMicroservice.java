package Microservices.Ensemble;


import Evaluation.ClassificationEvaluationService;
import Evaluation.RegressionEvaluationService;
import Structure.EnsembleResult;
import Structure.PartialPrediction;
import Serdes.GeneralFormat.GeneralSerde;
import Serdes.Init.ListOfPartialPredictionSerde;

import helperClasses.DynamicCountHolder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A standalone microservice that aggregates partial predictions (e.g., from 3 algorithms)
 * into a final EnsembleResult. It listens on 'EnsembleTopicForData-<streamId>' and writes
 * final results to 'OutputTopicForData-<streamId>'.
 */
public class EnsembleAggregatorMicroservice {
    private final String streamId;
    private  Long expectedNumOfAlgorithms;  // e.g. 3
    private KafkaStreams streams;
    private final AtomicLong totalRecordsProcessed;
    private long startTime;
    private long lastLogTime;

    private String taskType;

    String target;

    private final DynamicCountHolder countHolder;
    public EnsembleAggregatorMicroservice(String streamId,String target,String taskType, Long initialCount) {
        this.streamId = streamId;
       // this.expectedNumOfAlgorithms = expectedNumOfAlgorithms;
        totalRecordsProcessed = new AtomicLong(0);
        this.countHolder = new DynamicCountHolder();
        this.countHolder.set(initialCount);
        this.target=target;
        this.taskType=taskType;
    }

    /**
     * Builds and starts the Kafka Streams application for ensemble aggregation.
     */
    public void start() {

        this.startTime = System.currentTimeMillis();
        this.lastLogTime = this.startTime; // Initialize lastLogTime


        StreamsBuilder builder = new StreamsBuilder();

        // 1) Input partial predictions topic, e.g. "EnsembleTopicForData-0"
        String ensembleInputTopic = "EnsembleTopicForData-" + streamId +"-"+ target;
        // 2) Output final ensemble topic, e.g. "OutputTopicForData-0"
        String outputTopic = "OutputTopicForData-" + streamId + "-"+ target;

        System.out.println("Aggregator For streamid:"+streamId +" and target :" +target +" is for: "+taskType);


        // Serdes
        Serde<String> stringSerde = Serdes.String();
        Serde<PartialPrediction> partialPredictionSerde = new GeneralSerde<>(PartialPrediction.class);
        Serde<EnsembleResult> ensembleResultSerde = new GeneralSerde<>(EnsembleResult.class);



        // KStream of partial predictions keyed by recordId
        KStream<String, PartialPrediction> partialStream = builder.stream(
                ensembleInputTopic,
                Consumed.with(stringSerde, partialPredictionSerde)
        );

         //Build a state store for accumulating partial predictions:
        StoreBuilder<KeyValueStore<String, List<PartialPrediction>>> storeBuilder =
                Stores.keyValueStoreBuilder(
                       Stores.persistentKeyValueStore("ensemble-agg-store"),
                        stringSerde,
                        new ListOfPartialPredictionSerde()
               );
       builder.addStateStore(storeBuilder);


      //   StoreBuilder<KeyValueStore<String,Map<String,PartialPrediction>>> storeBuilder =
        //    Stores.keyValueStoreBuilder(
        //  Stores.persistentKeyValueStore("ensemble-agg-store"),
          // stringSerde,
        //  new MapPartialPredictionsSerde()
          //  );
       //   builder.addStateStore(storeBuilder);

/*
   if(taskType.equalsIgnoreCase("Classification"))
   {
       ClassificationEvaluationService eval = new ClassificationEvaluationService(streamId,target);
       eval.cleanUp();
       eval.start();
   }
   else {
       RegressionEvaluationService eval = new RegressionEvaluationService(streamId,target);
       eval.cleanUp();
       eval.start();
   }

 */




        // Use a custom processor that collects partial predictions and emits EnsembleResult
        KStream<String, EnsembleResult> finalEnsembleStream = partialStream.process(
                () -> new EnsembleAggregatorProcessor("ensemble-agg-store", this.countHolder,this.taskType),
                "ensemble-agg-store"
        );

        // Write final results to "OutputTopicForData-<streamId>"
        finalEnsembleStream.peek((k, v) -> recordProcessed())
                .to(outputTopic, Produced.with(stringSerde, ensembleResultSerde));

        // Build the topology and start Streams
        this.streams = new KafkaStreams(builder.build(), getStreamsConfig());
        this.streams.start();
    }




    public void updateCount(long newCount) {
        this.countHolder.set(newCount);

    }

    /**
     * Stops the Kafka Streams application.
     */
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

    /**
     * Optionally, cleans up local state directories. Typically used only in dev.
     */
    public void cleanUp() {
        if (streams != null) {
            streams.cleanUp();
        }
    }


    private Properties getStreamsConfig() {
        Properties props = new Properties();
        // Must be unique for each aggregator instance
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"EnsembleAggregatorMicroserviceFor-" + streamId+"-"+ target);

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

       // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"broker:9092");

        //props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/dataset/tmp/kafka-streams");
      //  props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
       props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        // read from earliest so we pick up partial predictions
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        return props;
    }



    private void recordProcessed() {
        long currentCount = totalRecordsProcessed.incrementAndGet();
        if (currentCount % 10000 == 0) {
            long currentTime = System.currentTimeMillis();
            double totalElapsedSeconds = (currentTime - startTime) / 1000.0;
            double totalThroughput = currentCount / totalElapsedSeconds;

            double deltaSeconds = (currentTime - lastLogTime) / 1000.0;
            double instantThroughput = 10000.0 / deltaSeconds;

            System.out.printf(
                    "Throughput EXIT : : %.2f records/second (Processed: %d records in %.2f seconds) with %d threads%n",
                    totalThroughput, currentCount, totalElapsedSeconds,2

            );

            System.out.printf("Instant Throughput EXIT (last 10k records): %.2f records/second%n",
                    instantThroughput);

            lastLogTime = currentTime; // Update lastLogTime to current time
        }
    }

    // Optional main() if you want to run this aggregator standalone via command line
    public static void main(String[] args) {
        // Example usage: aggregator for streamId="0", expecting partials from 3 algorithms
       // EnsembleAggregatorMicroservice aggregator = new EnsembleAggregatorMicroservice("0", 3);
       // aggregator.cleanUp(); // only in dev
       // aggregator.start();

        // Add shutdown hook for graceful exit
       // Runtime.getRuntime().addShutdownHook(new Thread(aggregator::stop));
    }
}
