
package Microservices.MLAlgorithmMicroService;

import Configuration.EnvironmentConfiguration;
import Microservices.Processors.PredictionProcessor;
import Structure.DataStructure;
import Structure.PartialPrediction;
import mlAlgorithms.MLAlgorithm;
import Microservices.Processors.BatchTrainingProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import Serdes.GeneralFormat.GeneralSerde;
import Serdes.MLAlgorithmSerde;

import java.time.Duration;
import java.util.*;

public class GenericMLAlgorithmMicroservice {
    // private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers1();
    private final String algorithmType;
    private final int algorithmId;
    private final KafkaStreams streams;
    private final String dataTopicName;
    private final String predTopicName;
    private final String ensembleTopicName;
    private final String outputTopicName;
    private final String modelStoreName;
    private final String PredictionStoreName;
    private final String target;
    Serde<MLAlgorithm> modelSerde;

    Serde<DataStructure> dataSerde ;
    Serde<PartialPrediction> partialPredictionSerde;
    private final String streamID;
    // private final String uniqueModelId;


    private final String  microserviceID;
    private final String taskType;
    private final Map<String, Object> hyperParams;

    public GenericMLAlgorithmMicroservice(String algorithmType, int algorithmId, String streamId, String target, String taskType, String microserviceID, Map<String,Object> hyperParams)  {

        //  this.uniqueModelId = streamId +"-" + target+ "-" + algorithmType + "- " + UUID.randomUUID();

        this.microserviceID =microserviceID;

        this.algorithmType = algorithmType;
        this.algorithmId = algorithmId;


        this.taskType=taskType;


        this.streamID = streamId;
        this.target = target;

        this.hyperParams = (hyperParams == null) ? new HashMap<>() : hyperParams;



        System.out.println("GenericMLAlgorithmMicroservice algorithmId: " + this.algorithmId);
        System.out.println(" UNIQUE MODEL ID: " + microserviceID);

        //  this.dataTopicName = "DataTopicMAlgorithm-" + streamId ;
        //this.predTopicName = "PredTopicMAlgorithm-" + streamId;

        // this.dataTopicName = "DataTopic-" + streamId;
        // this.predTopicName = "PredTopic-" + streamId;

        this.dataTopicName = "DataTopic-" + streamId;
        this.predTopicName = "PredTopic-" + streamId;

        this.ensembleTopicName = "EnsembleTopicForData-" + streamId+ "-" +target;
        this.outputTopicName = "OutputTopicForData-" + streamId+ "-" +target;

        this.modelStoreName = "Model"+ algorithmType + "STORE";
        this.PredictionStoreName ="Predictions" + algorithmType+ "STORE"; ;

        dataSerde = new GeneralSerde<>(DataStructure.class);
        partialPredictionSerde =  new GeneralSerde<>(PartialPrediction.class);

        // + "_" + streamId;

        modelSerde = new MLAlgorithmSerde();


        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, MLAlgorithm>> modelStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(modelStoreName),
                Serdes.String(),
                modelSerde
        );


        StoreBuilder<KeyValueStore<String, MLAlgorithm>> PredictionStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PredictionStoreName),
                Serdes.String(),
                modelSerde
        );


        builder.addStateStore(modelStoreBuilder);

        builder.addStateStore(PredictionStoreBuilder);


        setupModelUpdateStream(builder);


        // Build the topology
        setupTrainingStream(builder);
        //Serde<String> stringSerde = Serdes.String();
        //Serde<DataStructure> dataSerde = new GeneralSerde<>(DataStructure.class);

        // KStream<String, DataStructure> trainingStream = builder.stream(dataTopicName, Consumed.with(Serdes.String(), dataSerde));

        setupPredictionStream(builder);


        //  KStream<String, DataStructure> predStream = builder.stream(predTopicName, Consumed.with(Serdes.String(), dataSerde));

        // Build and start the Kafka Streams instance
        this.streams = new KafkaStreams(builder.build(), getStreamsConfig());

        System.out.println("TOPOLOGY Microservice:" +builder.build().describe());
    }

    public void start() {

        streams.start();
    }

    public void clear() {

        streams.cleanUp();
    }

    public void stop() {
        streams.close();
    }

    private Properties getStreamsConfig() {

        Properties props = new Properties();
        // props.put(StreamsConfig.APPLICATION_ID_CONFIG, algorithmId + "-Microservice-For-" + streamID + "-On-" + target);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,1);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG ,"MicroService-"+ microserviceID);
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, algorithmId + "-Microservice-" +streamID);
        // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,EnvironmentConfiguration.getBootstrapServers());

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/dataset/tmp/kafka-streams");
        props.put(StreamsConfig.STATE_DIR_CONFIG, EnvironmentConfiguration.getTempDir());

        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024); // e.g. 5 MB

        //props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2097152"); //4097152 2MB

        // props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"6097152 "); //4097152 2MB

        // Try 15 MB, for example
        // props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "15728640");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    private void setupTrainingStream(StreamsBuilder builder) {
        //Serde<String> stringSerde = Serdes.String();
        //Serde<DataStructure> dataSerde = new GeneralSerde<>(DataStructure.class);

        // State store to hold the mode

        KStream<String, DataStructure> trainingStream = builder.stream(dataTopicName, Consumed.with(Serdes.String(), dataSerde));
        // KStream<String, DataStructure> predictionStream = builder.stream(predTopicName, Consumed.with(stringSerde, dataSerde));

        trainingStream.process(() -> // new TrainingProcessor1(modelStoreName, uniqueModelId, algorithmType, algorithmId, streamID, target), modelStoreName);
                new  BatchTrainingProcessor(Duration.ofMillis(Long.parseLong(EnvironmentConfiguration.getTrainingBatchTime())),modelStoreName,
                        microserviceID, algorithmType, algorithmId, streamID, target,taskType,hyperParams), modelStoreName);
    }


    private void setupPredictionStream(StreamsBuilder builder) {
        //Serde<String> stringSerde = Serdes.String();
        // Serde<DataStructure> dataSerde = new GeneralSerde<>(DataStructure.class);
        // Serde<PartialPrediction> partialPredictionSerde = new GeneralSerde<>(PartialPrediction.class);



        KStream<String, DataStructure> predictionStream = builder.stream(predTopicName, Consumed.with(Serdes.String(), dataSerde));

        predictionStream.process(() -> new PredictionProcessor(Duration.ofMillis(Long.parseLong(EnvironmentConfiguration.getPredictionBatchTime())),modelStoreName, microserviceID, algorithmType, algorithmId, streamID, target,taskType), modelStoreName)
                .to(ensembleTopicName, Produced.with(Serdes.String(), partialPredictionSerde));

    }

    private void setupModelUpdateStream(StreamsBuilder builder) {

        // A Serde for the raw bytes or for some “ModelUpdateEvent” object
        //  Serde<byte[]> bytesSerde = Serdes.ByteArray();

        KStream<String, MLAlgorithm> updatesStream = builder.stream(
                "model-updates-topic",
                Consumed.with(Serdes.String(), modelSerde)
        );

        // Now filter or check if it belongs to me
        // (the simplest approach is to let *all* microservices see all keys,
        // but only apply them if key == uniqueModelId.
        // Or if you do a partitioning scheme, that's optional.)
        updatesStream
                .filter((key, value) -> key != null && key.equals(this.microserviceID))
                .process(() -> new Processor<String, MLAlgorithm, Void, Void>() {
                    private KeyValueStore<String, MLAlgorithm> modelStore;

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        this.modelStore = context.getStateStore(modelStoreName);
                    }

                    @Override
                    public void process(Record<String, MLAlgorithm> record) {

                        MLAlgorithm model = record.value();
                        if (model != null) {
                            System.out.println("Updating model: "+microserviceID);
                            // MLAlgorithm loaded = deserializeModel(modelBytes);
                            // Overwrite the store so training/prediction sees it
                            modelStore.put(microserviceID, model);

                            System.out.println("setupModelUpdateStream => overwrote store with loaded model for "
                                    + microserviceID
                            );
                        }

                    }

                    @Override
                    public void close() {}

                }, modelStoreName);
    }

}





