package Evaluation;

import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import Structure.EnsembleResult;
import Serdes.GeneralFormat.GeneralSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.util.Properties;

/**
 * Evaluates a regression ensemble:
 *   joins <finalPrediction> with the true numeric target,
 *   keeps running MAE / MSE / RMSE in a state‑store.
 */
public class RegressionEvaluationService {

    // ─────────────────────────── Kafka config ────────────────────────────
    private static final String APPLICATION_ID = "RegressionEval";
    private static final String BOOTSTRAP      = EnvironmentConfiguration.getBootstrapServers();
    private static final String STORE_NAME     = "reg-metrics-store";

    private final String aggregatorKey;
    private final String target ;
    final String PRED_INPUT_TOPIC;
    final String ENSEMBLE_INPUT_TOPIC;

    // ─────────────────────────── Metric holder ───────────────────────────
    public static class RegCounters {
        // running totals
        private long   n         = 0L;
        private double sumErr    = 0.0;   // Σ|ŷ − y|
        private double sumErr2   = 0.0;   // Σ(ŷ − y)²

        public RegCounters() {}                  // ←needed by Jackson

        /* Getters / setters */
        public long   getN()       { return n; }
        public void   setN(long n) { this.n = n; }

        public double getSumErr()    { return sumErr; }
        public void   setSumErr(double v) { this.sumErr = v; }

        public double getSumErr2()   { return sumErr2; }
        public void   setSumErr2(double v){ this.sumErr2 = v; }

        /* Update with one example */
        public void update(double trueVal, double predVal){
            n++;
            double err  = Math.abs(predVal - trueVal);
            double err2 = err * err;

            sumErr  += err;
            sumErr2 += err2;
        }

        /* Live metrics */
        public double mae (){ return (n==0)?0.0 :  sumErr  / n; }
        public double mse (){ return (n==0)?0.0 :  sumErr2 / n; }
        public double rmse(){ return Math.sqrt(mse()); }

        @Override public String toString(){
            return String.format("[count=%d, MAE=%.6f, MSE=%.6f, RMSE=%.6f]",
                    n, mae(), mse(), rmse());
        }
    }

    // ─────────────────────────── SerDes ───────────────────────────
    private final Serde<RegCounters> counterSerde   = new GeneralSerde<>(RegCounters.class);
    private final Serde<EnsembleResult> resultSerde = new GeneralSerde<>(EnsembleResult.class);
    private final Serde<DataStructure>  dataSerde   = new GeneralSerde<>(DataStructure.class);

    private final KafkaStreams streams;

    public RegressionEvaluationService(String aggregatorKey, String target){

        this.aggregatorKey=aggregatorKey;
        this.target=target;

        this.PRED_INPUT_TOPIC = "PredTopic-"+aggregatorKey;
        this.ENSEMBLE_INPUT_TOPIC = "OutputTopicForData-" + aggregatorKey + "-"+ target;

        StreamsBuilder builder = new StreamsBuilder();

        /* ---- state‑store ------------------------------------------------ */
        StoreBuilder<KeyValueStore<String,RegCounters>> storeB =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        counterSerde
                );
        builder.addStateStore(storeB);

        /* ---- read streams ---------------------------------------------- */
        KStream<String,EnsembleResult> ens = builder.stream(
                ENSEMBLE_INPUT_TOPIC, Consumed.with(Serdes.String(), resultSerde));

        KStream<String,DataStructure>  src = builder.stream(
                        PRED_INPUT_TOPIC, Consumed.with(Serdes.String(), dataSerde))
                .selectKey((k,v)->v.getRecordID());

        /* ---- join by recordID ------------------------------------------ */
        Duration window = Duration.ofHours(24);

        KStream<String,Tuple> joined = ens.join(
                src,
                (pred,orig)-> {
                    double trueVal = toDouble(orig.getFields().get("price"));
                    double predVal = toDouble(pred.getFinalPrediction());


                    return new Tuple(trueVal,predVal);
                },
                JoinWindows.ofTimeDifferenceAndGrace(window,Duration.ofHours(1)),
                StreamJoined.with(Serdes.String(), resultSerde, dataSerde)
        );


        /* ---- update metrics -------------------------------------------- */

        joined
                .filter((key, tpl) ->
                        !Double.isNaN(tpl.trueVal) && !Double.isNaN(tpl.predVal))

                .process(
                ()-> new Processor<String,Tuple,Void,Void>(){
                    private KeyValueStore<String,RegCounters> kv;
                    private long processed=0;

                    @Override public void init(ProcessorContext<Void,Void> ctx){
                        kv = ctx.getStateStore(STORE_NAME);
                    }
                    @Override public void process(Record<String,Tuple> rec){
                        RegCounters c = kv.get("GLOBAL");
                        if(c==null) c = new RegCounters();

                        if (Double.isNaN(rec.value().trueVal) || Double.isNaN(rec.value().predVal)) {
                            return;
                        }

                        c.update(rec.value().trueVal, rec.value().predVal);
                        kv.put("GLOBAL",c);

                        if(++processed % 10_000 ==0)
                            System.out.println("Updated metrics => "+c);
                    }
                    @Override public void close(){}
                },
                STORE_NAME
        );

        streams = new KafkaStreams(builder.build(), props());
    }

    private static double toDouble(Object o) {
        if (o == null) {
            return Double.NaN;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        String s = o.toString().trim();
        if (s.isEmpty()) {
            // no data → treat as missing
            return Double.NaN;
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ex) {
            // malformed number → log once or ignore
            System.err.println("Warning: cannot parse '" + s + "' into double; returning NaN");
            return Double.NaN;
        }
    }



    /* ------------ helpers -----------------------------------------------*/
    private static class Tuple{
        final double trueVal, predVal;
        Tuple(double t,double p){ trueVal=t; predVal=p; }
    }

    private Properties props(){
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,  APPLICATION_ID);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(StreamsConfig.STATE_DIR_CONFIG, "C:/dataset/tmp/kafka-streams");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return p;
    }

    /* ------------ life‑cycle --------------------------------------------*/
    public void start(){ streams.start(); }
    public void stop (){ streams.close(); }
    public void cleanUp() {
        streams.cleanUp();
    }

    public static void main(String[] args){
        RegressionEvaluationService svc = new RegressionEvaluationService("EURTRY","price");
        svc.cleanUp();     // dev‑only
        svc.start();
        Runtime.getRuntime().addShutdownHook(new Thread(svc::stop));
    }
}
