package Microservices.Ensemble;

import Structure.EnsembleResult;
import Structure.PartialPrediction;
import helperClasses.DynamicCountHolder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EnsembleAggregatorProcessor implements Processor<String, PartialPrediction, String, EnsembleResult> {

    private ProcessorContext<String, EnsembleResult> context;
    private KeyValueStore<String, List<PartialPrediction>> kvStore;
    private final String storeName;
   // private final Long requiredCount;

    private long totalRecordsProcessed;

    private final DynamicCountHolder countHolder;

    private long startTime;
    private long lastLogTime; // Added declaratio

    private String taskType;


   // public EnsembleAggregatorProcessor(String storeName, Long requiredCount)
     public EnsembleAggregatorProcessor(String storeName, DynamicCountHolder countHolder,String taskType){
        this.storeName = storeName;
      //  this.requiredCount = requiredCount;

        this.countHolder= countHolder;

        this.taskType=taskType;
    }

    @Override
    public void init(ProcessorContext<String, EnsembleResult> context) {
        this.context = context;
        this.kvStore = context.getStateStore(storeName);
        context.schedule(
                Duration.ofSeconds(30),   // or 5, or 30, etc.
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
                    flushStuckRecords();
                }
        );
    }

    @Override
    public void process(Record<String, PartialPrediction> record) {

        String recordId = record.key();
        PartialPrediction incoming = record.value();

        if (incoming == null)
            return;

        List<PartialPrediction> list = kvStore.get(recordId);
        if (list == null) {
            list = new ArrayList<>();
        }

// Only add if this algorithmType is not already in the list
        boolean alreadyExists = false;

        for (PartialPrediction p : list) {
            // Possibly also check target if multiple targets exist
            /*
            if (p.getAlgorithmType().equals(incoming.getAlgorithmType() )) {
                alreadyExists = true;
                break;
            }
             */
            /*
            if (p.getAlgorithmType().equals(incoming.getAlgorithmType())
                    && p.getHyperParams().equals(incoming.getHyperParams())) {
                alreadyExists = true;
                break;
            }

             */


        }

        //if (!alreadyExists) {
            list.add(incoming);
            kvStore.put(recordId, list);
       // }

// Now if list.size() == requiredCount, do final logic...


        totalRecordsProcessed++;


        //if(totalRecordsProcessed % 10000 == 0) {
          //  System.out.println("Active Algorithms :" +countHolder.get());
        //}

        if (list.size() == countHolder.get()) {

            String finalPrediction;

            if ("Classification".equalsIgnoreCase(taskType)) {
                finalPrediction = doMajorityVote(list);
            } else {
                finalPrediction = doAverage(list);
            }


            // Build an EnsembleResult
             EnsembleResult result = new EnsembleResult(recordId, finalPrediction, list);


           // EnsembleResult2 result = new EnsembleResult2(recordId, finalPrediction);

            // Forward with the same key (recordId),
            // but value = our EnsembleResult object
            context.forward(new Record<>(recordId, result, record.timestamp()));

            // Optionally remove from store
             kvStore.delete(recordId);

        }

    }

    private void flushStuckRecords() {
        try (KeyValueIterator<String, List<PartialPrediction>> iter = kvStore.all()) {
            while (iter.hasNext()) {
                KeyValue<String, List<PartialPrediction>> entry = iter.next();
                String recordId = entry.key;
                List<PartialPrediction> partials = entry.value;
                if (partials == null) continue;
                // If the aggregator’s membership is now 2, you might do:
                if (partials.size() == countHolder.get()) {
                    // The partial set is “complete” by the new membership
                    String finalPrediction;
                    if ("classification".equalsIgnoreCase(taskType)) {
                        finalPrediction = doMajorityVote(partials);
                    } else {
                        finalPrediction = doAverage(partials);
                    }

                    EnsembleResult result = new EnsembleResult(recordId, finalPrediction, partials);
                    //EnsembleResult result = new EnsembleResult2(recordId, finalPrediction);
                    context.forward(new Record<>(recordId, result, System.currentTimeMillis()));

                    // Optionally remove the partial set to avoid re-processing
                    kvStore.delete(recordId);
                }
            }
        }
    }


    private String doWeightedMajorityVote(List<PartialPrediction> partials) {
        // Map from algorithm type to a weight, e.g., based on historical accuracy.
        Map<String, Double> algorithmWeights = new HashMap<>();
        // These weights can be determined based on evaluation; adjust as needed.
        algorithmWeights.put("Decision-Trees", 0.80);
        algorithmWeights.put("kNN", 0.67);
        algorithmWeights.put("Naive-Bayes", 0.54);

        Map<String, Double> weightedVotes = new HashMap<>();
        for (PartialPrediction p : partials) {
            // Get the weight assigned for this algorithm
            double weight = algorithmWeights.getOrDefault(p.getAlgorithmType(), 1.0);
            // Merge the weight for the predicted label
            weightedVotes.merge(p.getPrediction(), weight, Double::sum);
        }
        return weightedVotes.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
    }




    private String doMajorityVote(List<PartialPrediction> partials) {


        Map<String, Integer> freqMap = new HashMap<>();
        for (PartialPrediction p : partials) {
            freqMap.merge(p.getPrediction(), 1, Integer::sum);
        }
        return freqMap.entrySet()
                .stream()
                .max(Map.Entry.comparingByValue())
                .get()
                .getKey();
    }


    private String doAverage(List<PartialPrediction> partials) {
        // Convert partials to double, compute average
        double sum = 0.0;
        int count = 0;
        for (PartialPrediction p : partials) {
            try {
                double val = Double.parseDouble(p.getPrediction());
                sum += val;
                count++;
            } catch (NumberFormatException e) {
                // skip or handle missing
            }
        }
        if (count == 0) {
            return "NaN"; // or some fallback
        }
        double avg = sum / count;
        return String.valueOf(avg);
    }

    @Override
    public void close() {}
}