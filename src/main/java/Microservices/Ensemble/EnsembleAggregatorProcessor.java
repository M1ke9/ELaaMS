package Microservices.Ensemble;

import Microservices.Router.MicroServiceInfo;
import Microservices.Router.MicroserviceDetails;
import Structure.EnsembleResult;
import Structure.PartialPrediction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp; // *** IMPORT THIS ***

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class EnsembleAggregatorProcessor implements Processor<String, PartialPrediction, String, EnsembleResult> {

    private ProcessorContext<String, EnsembleResult> context;
    private KeyValueStore<String, List<PartialPrediction>> partialsStore;
    // *** FIX 1: The store holds ValueAndTimestamp objects, not MicroServiceInfo directly. ***
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<MicroServiceInfo>> membershipStore;

    private final String partialsStoreName;
    private final String membershipStoreName;
    private final String streamId;
    private final String target;

    public EnsembleAggregatorProcessor(String partialsStoreName, String membershipStoreName, String streamId, String target) {
        this.partialsStoreName = partialsStoreName;
        this.membershipStoreName = membershipStoreName;
        this.streamId = streamId;
        this.target = target;
    }

    @Override
    public void init(ProcessorContext<String, EnsembleResult> context) {
        this.context = context;
        this.partialsStore = context.getStateStore(partialsStoreName);
        this.membershipStore = context.getStateStore(membershipStoreName);

        context.schedule(
                Duration.ofSeconds(30),
                PunctuationType.WALL_CLOCK_TIME,
                this::reconcileAndCleanup
        );
    }

    @Override
    public void process(Record<String, PartialPrediction> record) {
        String recordId = record.key();
        PartialPrediction incoming = record.value();

        if (incoming == null || incoming.getAlgorithmType() == null || incoming.getHyperParams() == null) {
            return;
        }

        List<PartialPrediction> predictions = partialsStore.get(recordId);
        if (predictions == null) {
            predictions = new ArrayList<>();
        }

        String incomingCompositeKey = incoming.getAlgorithmType() + "|" + incoming.getHyperParams();
        boolean alreadyExists = predictions.stream()
                .anyMatch(p -> (p.getAlgorithmType() + "|" + p.getHyperParams()).equals(incomingCompositeKey));

        if (!alreadyExists) {
            predictions.add(incoming);
            partialsStore.put(recordId, predictions);
        }

        checkForCompletion(recordId, predictions);
    }

    private void checkForCompletion(String recordId, List<PartialPrediction> receivedPredictions) {
        // *** FIX 2: Get the ValueAndTimestamp object first, then extract the value. ***
        ValueAndTimestamp<MicroServiceInfo> valueAndTimestamp = membershipStore.get(this.streamId);
        MicroServiceInfo currentMembership = (valueAndTimestamp == null) ? null : valueAndTimestamp.value();

        if (currentMembership == null) {
            return;
        }

        Set<String> requiredPredictorKeys = currentMembership.getAllMicroservices().stream()
                .filter(details -> this.target.equals(details.getTarget()))
                .map(details -> details.getAlgorithmType() + "|" + details.getHyperParams().toString())
                .collect(Collectors.toSet());

        if (requiredPredictorKeys.isEmpty()) {
            return;
        }

        Set<String> receivedPredictorKeys = receivedPredictions.stream()
                .map(p -> p.getAlgorithmType() + "|" + p.getHyperParams())
                .collect(Collectors.toSet());

        if (receivedPredictorKeys.containsAll(requiredPredictorKeys)) {
            List<PartialPrediction> relevantPredictions = receivedPredictions.stream()
                    .filter(p -> {
                        String key = p.getAlgorithmType() + "|" + p.getHyperParams();
                        return requiredPredictorKeys.contains(key);
                    })
                    .collect(Collectors.toList());

            String taskType = currentMembership.getAllMicroservices().stream()
                    .filter(details -> this.target.equals(details.getTarget()))
                    .findFirst()
                    .map(details -> details.getTaskType())
                    .orElse("Classification");

            String finalPrediction;
            if ("Classification".equalsIgnoreCase(taskType)) {
                finalPrediction = doMajorityVote(relevantPredictions);
            } else {
                finalPrediction = doAverage(relevantPredictions);
            }

            EnsembleResult result = new EnsembleResult(recordId, finalPrediction, relevantPredictions);
            context.forward(new Record<>(recordId, result, System.currentTimeMillis()));
            partialsStore.delete(recordId);
        }
    }

    private void reconcileAndCleanup(long timestamp) {
        try (KeyValueIterator<String, List<PartialPrediction>> iter = partialsStore.all()) {
            while (iter.hasNext()) {
                KeyValue<String, List<PartialPrediction>> entry = iter.next();
                if (entry.value != null) {
                    checkForCompletion(entry.key, entry.value);
                }
            }
        }
    }

    private String doMajorityVote(List<PartialPrediction> partials) {
        if (partials == null || partials.isEmpty()) return null;
        return partials.stream()
                .map(PartialPrediction::getPrediction)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(p -> p, Collectors.counting()))
                .entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    private String doAverage(List<PartialPrediction> partials) {
        if (partials == null || partials.isEmpty()) return "NaN";
        return String.valueOf(partials.stream()
                .map(PartialPrediction::getPrediction)
                .filter(Objects::nonNull)
                .mapToDouble(p -> {
                    try {
                        return Double.parseDouble(p);
                    } catch (NumberFormatException e) {
                        return Double.NaN;
                    }
                })
                .filter(d -> !Double.isNaN(d))
                .average()
                .orElse(Double.NaN));
    }

    @Override
    public void close() {
        // No-op
    }
}
