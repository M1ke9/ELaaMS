package Evaluation;

import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import Structure.EnsembleResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import Serdes.GeneralFormat.GeneralSerde;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Demonstrates a multi-class confusion matrix in Kafka Streams:
 *  - We store a nested Map<String, Map<String, Long>>: confusionMatrix[actual][predicted].
 *  - We compute macro-F1 and micro-F1 from that matrix.
 *
 * It joins final ensemble predictions from OutputTopicForData-<ID> with
 * the original data from PredTopic-<ID>, by recordId, to get (trueLabel, predictedLabel).
 */
public class ClassificationEvaluationService {

    private final KafkaStreams streams;
    private static final String APPLICATION_ID = "EvaluationMicroservice";
    private static final String BOOTSTRAP = "localhost:29092";
    private final String aggregatorKey;
    private final String target ;
    final String PRED_INPUT_TOPIC;
    final String ENSEMBLE_INPUT_TOPIC;


    // Topics (adjust as needed):
   // private static final String PRED_INPUT_TOPIC = "PredTopic-SHIPS1-Forex";
   // private static final String ENSEMBLE_INPUT_TOPIC = "OutputTopicForData-SHIPS1-Forex-status";

    private static final String EVAL_STORE_NAME = "evaluation-store";

    public static class MetricCounters {

        // totalSamples = total # predictions we have processed
        private long totalSamples = 0;
        // correctPredictions = sum of diagonal (quick way to measure overall accuracy)
        private long correctPredictions = 0;

        // confusionMatrix[actualClass][predictedClass] = count of how many times
        // actualClass was predicted as predictedClass
        private Map<String, Map<String, Long>> confusionMatrix = new HashMap<>();

        public MetricCounters() {}

        // ---- getters / setters (needed if you're storing in a StateStore with Jackson) ----
        public long getTotalSamples() { return totalSamples; }
        public void setTotalSamples(long totalSamples) { this.totalSamples = totalSamples; }

        public long getCorrectPredictions() { return correctPredictions; }
        public void setCorrectPredictions(long correctPredictions) { this.correctPredictions = correctPredictions; }

        public Map<String, Map<String, Long>> getConfusionMatrix() { return confusionMatrix; }
        public void setConfusionMatrix(Map<String, Map<String, Long>> confusionMatrix) { this.confusionMatrix = confusionMatrix; }
        // --------------------------------------------------------------------------

        /**
         * Updates the confusion matrix for multi-class classification.
         * actual = the true label
         * predicted = the predicted label
         */
        public void update(String actual, String predicted) {
            totalSamples++;
            if (actual.equals(predicted)) {
                correctPredictions++;
            }

            // Insert into confusionMatrix
            confusionMatrix
                    .computeIfAbsent(actual, k -> new HashMap<>())
                    .merge(predicted, 1L, Long::sum);
        }

        /**
         * Overall accuracy = diagonal / total
         */
        public double accuracy() {
            if (totalSamples == 0) return 0.0;
            return (double) correctPredictions / totalSamples;
        }

        /**
         * Return all labels (classes) observed so far.
         * We'll gather them from confusionMatrix rows + columns.
         */
        private Set<String> allClasses() {
            Set<String> all = new HashSet<>(confusionMatrix.keySet());
            for (Map<String, Long> row : confusionMatrix.values()) {
                all.addAll(row.keySet());
            }
            return all;
        }

        /**
         * True positives for class c = confusionMatrix[c][c], if present
         */
        private long tp(String c) {
            return confusionMatrix
                    .getOrDefault(c, new HashMap<>())
                    .getOrDefault(c, 0L);
        }

        /**
         * False negatives for class c = sum of confusionMatrix[c][x for x != c]
         */
        private long fn(String c) {
            long sum = 0;
            Map<String, Long> row = confusionMatrix.getOrDefault(c, new HashMap<>());
            for (Map.Entry<String, Long> e : row.entrySet()) {
                String predicted = e.getKey();
                long count = e.getValue();
                if (!predicted.equals(c)) {
                    sum += count;
                }
            }
            return sum;
        }

        /**
         * False positives for class c = sum of confusionMatrix[y != c][c]
         */
        private long fp(String c) {
            long sum = 0;
            for (Map.Entry<String, Map<String, Long>> rowEntry : confusionMatrix.entrySet()) {
                String actualClass = rowEntry.getKey();
                if (!actualClass.equals(c)) {
                    Map<String, Long> predMap = rowEntry.getValue();
                    sum += predMap.getOrDefault(c, 0L);
                }
            }
            return sum;
        }

        /**
         * True negatives for class c = totalSamples - (TP + FN + FP)
         * (We don't explicitly store it in the matrix, we can compute it if needed.)
         */
        private long tn(String c) {
            long tpVal = tp(c);
            long fnVal = fn(c);
            long fpVal = fp(c);
            long sum = tpVal + fnVal + fpVal;
            return totalSamples - sum;
        }

        /**
         * Macro-F1 = average of per-class F1 across all classes
         */
        public double macroF1() {
            Set<String> classes = allClasses();
            if (classes.isEmpty()) return 0.0;

            double sumF1 = 0.0;
            for (String c : classes) {
                double f1c = f1ForClass(c);
                sumF1 += f1c;
            }
            return sumF1 / classes.size();
        }

        /**
         * F1 for a single class c
         */
        public double f1ForClass(String c) {
            double prec = precisionForClass(c);
            double rec  = recallForClass(c);
            if (prec + rec == 0.0) return 0.0;
            return 2.0 * (prec * rec) / (prec + rec);
        }

        /**
         * precision(c) = TP_c / (TP_c + FP_c)
         */
        public double precisionForClass(String c) {
            long tpVal = tp(c);
            long fpVal = fp(c);
            long denom = tpVal + fpVal;
            if (denom == 0) return 0.0;
            return (double) tpVal / denom;
        }

        /**
         * recall(c) = TP_c / (TP_c + FN_c)
         */
        public double recallForClass(String c) {
            long tpVal = tp(c);
            long fnVal = fn(c);
            long denom = tpVal + fnVal;
            if (denom == 0) return 0.0;
            return (double) tpVal / denom;
        }

        /**
         * Micro-F1 is computed from the global sums of TP, FP, FN across *all* classes.
         * microPrecision = sum(TP_c) / sum(TP_c + FP_c)
         * microRecall    = sum(TP_c) / sum(TP_c + FN_c)
         * microF1 = 2 * microPrecision * microRecall / (microPrecision + microRecall)
         *
         * In multi-class classification, microPrecision == microRecall == overall accuracy
         * if you define them in the standard way. We'll do the formula explicitly.
         */
        public double microF1() {
            // sum of all TP
            long sumTP = 0;
            // sum of all FP, FN
            long sumFP = 0;
            long sumFN = 0;

            Set<String> classes = allClasses();
            for (String c : classes) {
                sumTP += tp(c);
                sumFP += fp(c);
                sumFN += fn(c);
            }

            // microPrecision = sumTP / (sumTP + sumFP)
            double microPrec = 0.0;
            double denomP = sumTP + sumFP;
            if (denomP != 0) {
                microPrec = (double) sumTP / denomP;
            }

            // microRecall = sumTP / (sumTP + sumFN)
            double microRec = 0.0;
            double denomR = sumTP + sumFN;
            if (denomR != 0) {
                microRec = (double) sumTP / denomR;
            }

            if (microPrec + microRec == 0.0) {
                return 0.0;
            }
            return 2.0 * (microPrec * microRec) / (microPrec + microRec);
        }

        @Override
        public String toString() {
            return String.format(
                    "[Samples=%d, Accuracy=%.3f, MacroF1=%.3f, MicroF1=%.3f]",
                    totalSamples, accuracy(), macroF1(), microF1()
            );
        }
    }

    // Serdes
    private final Serde<MetricCounters> metricSerde = new GeneralSerde<>(MetricCounters.class);
    private final Serde<EnsembleResult> resultSerde = new GeneralSerde<>(EnsembleResult.class);
    private final Serde<DataStructure> dataSerde = new GeneralSerde<>(DataStructure.class);

    public ClassificationEvaluationService(String aggregatorKey, String target) {

     this.aggregatorKey=aggregatorKey;
      this.target=target;

        this.PRED_INPUT_TOPIC = "PredTopic-"+aggregatorKey;
        this.ENSEMBLE_INPUT_TOPIC = "OutputTopicForData-" + aggregatorKey + "-"+ target;



        StreamsBuilder builder = new StreamsBuilder();
        // 1) Build store for metrics
        StoreBuilder<KeyValueStore<String, MetricCounters>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EVAL_STORE_NAME),
                        Serdes.String(),
                        metricSerde
                );
        builder.addStateStore(storeBuilder);

        // 2) KStream of final ensemble predictions
        KStream<String, EnsembleResult> ensembleStream = builder.stream(
                ENSEMBLE_INPUT_TOPIC,
                Consumed.with(Serdes.String(), resultSerde)
        );

        // 3) KStream of original data w/ true label, re-keyed by recordId
        KStream<String, DataStructure> originalDataByRecordId = builder
                .stream(PRED_INPUT_TOPIC, Consumed.with(Serdes.String(), dataSerde))
                .selectKey((oldKey, dataVal) -> dataVal.getRecordID());

        // 4) Windowed join (e.g. 24h window). Adjust as needed if your data is older or real-time.
        Duration joinWindow = Duration.ofHours(24);

        KStream<String, EvaluationTuple> joinedStream = ensembleStream.join(
                originalDataByRecordId,
                (ensembleRes, originalData) -> {
                    String trueLabel = (String) originalData.getFields().get("status").toString();
                    String predicted = ensembleRes.getFinalPrediction();
                    return new EvaluationTuple(trueLabel, predicted);
                },
                JoinWindows.ofTimeDifferenceAndGrace(joinWindow, Duration.ofHours(1)),
                StreamJoined.with(Serdes.String(), resultSerde, dataSerde)
        );

        // 5) Update metrics in the store
        joinedStream.process(
                () -> new Processor<String, EvaluationTuple, Void, Void>() {
                    private KeyValueStore<String, MetricCounters> kvStore;
                    private long recordCounter = 0;

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        kvStore = context.getStateStore(EVAL_STORE_NAME);
                    }

                    @Override
                    public void process(Record<String, EvaluationTuple> record) {
                        String globalKey = "GLOBAL_METRICS";
                        MetricCounters counters = kvStore.get(globalKey);
                        if (counters == null) {
                            counters = new MetricCounters();
                        }

                        // Update the multi-class confusion matrix
                        counters.update(record.value().trueLabel, record.value().predictedLabel);
                        kvStore.put(globalKey, counters);

                        recordCounter++;
                        // Print every 10k records
                        if (recordCounter % 10000 == 0) {
                            System.out.println("Updated metrics => " + counters);
                        }
                    }

                    @Override
                    public void close() {}
                },
                EVAL_STORE_NAME
        );

        // Build the topology
        Topology topology = builder.build();
        this.streams = new KafkaStreams(topology, getStreamsConfig());
    }

    public void start() {
        streams.start();
    }

    public void stop() {
        streams.close();
    }

    public void cleanUp() {
        streams.cleanUp();
    }

    private Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ClassificationEvaluationService"+"-"+aggregatorKey+"-"+target);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        //props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/dataset/tmp/kafka-streams");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        // For re-reading old data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }

    // Data holder for (trueLabel, predictedLabel)
    static class EvaluationTuple {
        String trueLabel;
        String predictedLabel;

        public EvaluationTuple(String trueLabel, String predictedLabel) {
            this.trueLabel = trueLabel;
            this.predictedLabel = predictedLabel;
        }
    }

    public static void main(String[] args) {

       ClassificationEvaluationService evalService = new ClassificationEvaluationService("SHIPS","status");
        evalService.cleanUp(); // dev only, wipes local state
        evalService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(evalService::stop));


    }
}
