package Microservices.Processors;

import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import Structure.PartialPrediction;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import mlAlgorithms.AMRules.MoaAMRulesRegressorModel;
import mlAlgorithms.Decision_Trees.MoaDecisionTreeModel;
import mlAlgorithms.Decision_Trees.MoaHATModel;
import mlAlgorithms.FIMTDD.MoaFIMTDDModel;
import mlAlgorithms.MLAlgorithm;
import mlAlgorithms.Naive_Bayes.MoaNaiveBayesModel;
import mlAlgorithms.Perceptron.MoaPerceptronModel;
import mlAlgorithms.RandomForest.MoaRandomForestModel;
import mlAlgorithms.RandomForest.MoaRandomForestRegressorModel;
import mlAlgorithms.SGD.MoaSGDModel;
import mlAlgorithms.kNN.MoaKNNModel;
// import moa.classifiers.lazy.kNN; // Not used directly here
import moa.core.Utils;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import scala.xml.Null;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Handles both classification and regression predictions using batch processing.
 * Prediction requests are buffered and processed periodically.
 */
public class PredictionProcessor implements Processor<String, DataStructure, String, PartialPrediction> {

    private final String modelStoreName;
    private final String algorithmType;
    private final int algorithmId;
    private final String streamID;
    private final String target;
    private final String uniqueModelId;
    private final String taskType; // Added: Needed for converting instance correctly


    private List<Record<String, DataStructure>> predictionBuffer; // Buffer for incoming prediction requests

    // --- Metrics ---
    private long totalPredictions = 0;
    private long correctPredictions = 0; // only for classification
    private double sumSquaredError = 0.0; // only for regression
    private long regressionCount = 0; // only for regression

    // --- Throughput Tracking ---
    private long predictionRecordsProcessed = 0; // Total records processed by this instance
    private long firstRecordTimeMs = -1; // Time of the first record processed (for final throughput)
    private long batchProcessingStartMillis = -1; // Start time for current throughput measurement interval

    private KeyValueStore<String, MLAlgorithm> modelStore;
    private ProcessorContext<String, PartialPrediction> context;

    // --- Throughput Logging to File ---
    private transient BufferedWriter throughputWriter;
   // private static final String THROUGHPUT_FILE_PATH = "/home/mmarketakis/Dataset/metrics/PredictionResults.txt"; // Make configurable if needed

    private static final String THROUGHPUT_FILE_PATH = EnvironmentConfiguration.getFilePathForThroughputPredict();
    // Define thresholds at which to log throughput (Adjust as needed)
    private static final long[] THROUGHPUT_THRESHOLDS = {100_000, 200_000, 260000,520000,800000,840000};
    private final Set<Long> loggedThresholds = new HashSet<>();
    private final long duration;

    public PredictionProcessor(Duration duration, // Added duration parameter
                               String modelStoreName,
                               String uniqueModelId,
                               String algorithmType,
                               int algorithmId,
                               String streamID,
                               String target,
                               String taskType) { // Added taskType
        this.duration = duration.toMillis();
        this.modelStoreName = modelStoreName;
        this.uniqueModelId = uniqueModelId;
        this.algorithmType = algorithmType;
        this.algorithmId = algorithmId;
        this.streamID = streamID;
        this.target = target;
        this.taskType = taskType; // Store taskType
    }

    @Override
    public void init(ProcessorContext<String, PartialPrediction> context) {
        this.context = context;
        this.modelStore = context.getStateStore(modelStoreName);
        this.predictionBuffer = new ArrayList<>(); // Initialize the buffer

        // --- Initialize the throughput writer ---
        try {
            // Open the file in append mode (true)
            throughputWriter = new BufferedWriter(new FileWriter(THROUGHPUT_FILE_PATH, true));
            System.out.println("PredictionProcessor[" + uniqueModelId + "]: Opened throughput log file: " + THROUGHPUT_FILE_PATH);
        } catch (IOException e) {
            System.err.println("PredictionProcessor[" + uniqueModelId + "]: FATAL: Could not open throughput log file: " + THROUGHPUT_FILE_PATH);
            e.printStackTrace();
            // Consider making this fatal or implementing a retry/fallback mechanism
            // throw new org.apache.kafka.streams.errors.StreamsException("Failed to open throughput log file", e);
        }

        // Schedule the batch processing method (processBatch)
        context.schedule(
                Duration.ofMillis(duration),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> processBatch() // Call the batch processing method
        );

    }

    @Override
    public void process(Record<String, DataStructure> record) {
        // Set first record time for overall throughput calculation
        if (firstRecordTimeMs < 0) {
            firstRecordTimeMs = System.currentTimeMillis();
        }

        // Start throughput timer on first record of a measurement cycle if not already started
        //if (batchProcessingStartMillis < 0) {
        //   System.out.println("ONCE");
        //  batchProcessingStartMillis = System.currentTimeMillis();
        //  }


        DataStructure dataInstance = record.value();
        if (dataInstance == null) {
            System.out.println("PredictionProcessor[" + uniqueModelId + "]: Received null record, skipping.");
            return; // Skip null records
        }

        // Always add the record to the buffer
        predictionBuffer.add(record);

        // Optional: Add size-based flush logic if needed (in addition to time)
        // if (predictionBuffer.size() >= BATCH_SIZE_LIMIT) {
        //     processBatch();
        // }
    }

    /**
     * Processes the buffered prediction requests.
     * Fetches the model once, predicts for all buffered instances, calculates metrics,
     * logs throughput, and forwards results.
     */
    private void processBatch() {
        if (predictionBuffer.isEmpty()) {
            // System.out.println("PredictionProcessor[" + uniqueModelId + "]: No records in buffer to process.");
            return; // Nothing to process
        }

        // 1) Try to get the model from the store ONCE for the batch
        MLAlgorithm model = modelStore.get(uniqueModelId);

        if (model == null) {
            // Model not ready yet, keep records buffered. They will be processed in the next punctuation cycle.
            System.out.println("PredictionProcessor[" + uniqueModelId + "]: Model not found in store. Keeping " + predictionBuffer.size() + " records buffered.");
            return;
        }


        if(model != null)
        {
            if (batchProcessingStartMillis < 0) {
                System.out.println("ONCE");
                batchProcessingStartMillis = System.currentTimeMillis();
            }
        }

        // Model is available, process the buffered records
        // System.out.println("PredictionProcessor[" + uniqueModelId + "]: Model found. Processing " + predictionBuffer.size() + " buffered records.");

        int processedInBatch = 0;
        InstancesHeader header = getModelHeader(model); // Get header once
        if (header == null) {
            System.err.println("PredictionProcessor[" + uniqueModelId + "]: FATAL: Cannot process batch, model header is null!");
            // Depending on requirements, might clear buffer or keep trying
            // predictionBuffer.clear(); // Option: Discard batch if header is missing
            return;
        }


        for (Record<String, DataStructure> record : predictionBuffer) {
            DataStructure data = record.value();
            if (data == null) continue; // Should have been caught in process, but double-check

            // 2) Convert DataStructure to MOA Instance
            // Pass header to avoid repeated lookups inside convertToInstance
            Instance instance = convertToInstance(data, model, header);
            if (instance == null) {
                System.err.println("PredictionProcessor[" + uniqueModelId + "]: Failed to convert record to instance, skipping. Record Key: " + record.key());
                continue; // Skip if conversion fails
            }

            // 3) Predict
            Object predictionResult = model.predict(instance); // Use the fetched model
            String recordId = record.value().getRecordID();


            if (predictionResult != null && recordId != null) {
                PartialPrediction partial = new PartialPrediction(
                        this.algorithmType,
                        predictionResult.toString(),
                        model.getHyperParams().toString()
                );

                context.forward(new Record<>(recordId, partial, record.timestamp()));
            }




            // 6) Update Metrics (based on task type)
            // updateMetrics(instance, predictionResult, header);

            predictionRecordsProcessed++; // Increment total processed count
            processedInBatch++;
        }

        // 7) Log Throughput periodically
        for (long threshold : THROUGHPUT_THRESHOLDS) {
            if (predictionRecordsProcessed >= threshold && !loggedThresholds.contains(threshold)) {
                logThroughput(threshold);

            }}
        // 8) Clear the buffer now that the batch is processed
        predictionBuffer.clear();

        // Optional: Log batch completion
        // if (processedInBatch > 0) {
        //     System.out.println("PredictionProcessor[" + uniqueModelId + "]: Processed batch of " + processedInBatch + " records.");
        // }
    }

    /**
     * Helper method to update prediction metrics (Accuracy for classification, MSE for regression).
     * Handles prediction results that are double[], Number, or String (for classification).
     */
    private void updateMetrics(Instance instance, Object predictionResult, InstancesHeader header) {
        if (instance.classIsMissing()) {
            // Cannot calculate metrics if the true class value is missing in the input instance
            // System.out.println("PredictionProcessor[" + uniqueModelId + "]: Skipping metrics update, actual class is missing."); // Optional debug
            return;
        }

        double actualValue = instance.classValue(); // Get the true value (numeric index) from the instance

        if (header.classAttribute().isNominal()) { // Classification
            totalPredictions++; // Count only instances where we have an actual value to compare
            double predictedClassIndex = -1.0; // Default to invalid index

            if (predictionResult instanceof double[]) {
                // Prediction result is a distribution, find the index with the highest probability
                double[] distribution = (double[]) predictionResult;
                if (distribution != null && distribution.length > 0) {
                    predictedClassIndex = Utils.maxIndex(distribution);
                } else {
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: Received empty or null distribution array for classification.");
                    return; // Cannot determine index
                }
            } else if (predictionResult instanceof Number) {
                // Prediction result is the class index directly
                predictedClassIndex = ((Number) predictionResult).doubleValue();
            } else if (predictionResult instanceof String) {
                // *** NEW: Handle String prediction result ***
                // Convert the predicted string label back to its numeric index
                String predictedLabel = (String) predictionResult;
                Attribute classAttr = header.classAttribute();
                int index = classAttr.indexOfValue(predictedLabel);
                if (index != -1) {
                    predictedClassIndex = index; // Found the index for the string label
                } else {
                    // The predicted string label wasn't found in the header's list of nominal values
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: Predicted string label '" + predictedLabel + "' not found in header for attribute '" + classAttr.name() + "'. Cannot calculate metric for this instance.");
                    return; // Cannot compare if label is unknown
                }
            } else {
                // Handle null or other unexpected types
                if (predictionResult == null) {
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: Prediction result is null for classification.");
                } else {
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: Unexpected prediction result type for classification: " + predictionResult.getClass().getName());
                }
                return; // Cannot compare if format is unknown or null
            }

            // Compare predicted index with actual index (only if predicted index is valid)
            if (predictedClassIndex >= 0) {
                if (Math.abs(predictedClassIndex - actualValue) < 1e-6) { // Use tolerance for double comparison
                    correctPredictions++;
                }
            } else {
                // This case should ideally not be reached if the above checks work, but good for safety
                System.err.println("PredictionProcessor[" + uniqueModelId + "]: Could not determine a valid predicted class index.");
            }

        } else if (header.classAttribute().isNumeric()) { // Regression
            totalPredictions++; // Count only instances where we have an actual value to compare
            regressionCount++;
            double predictedNumericValue;

            if (predictionResult instanceof Number) {
                predictedNumericValue = ((Number) predictionResult).doubleValue();
            } else {
                // Handle null or other unexpected types
                if (predictionResult == null) {
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: Prediction result is null for regression.");
                } else {
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: Unexpected prediction result type for regression: " + predictionResult.getClass().getName() + ". Expected a Number.");
                }
                return; // Cannot calculate error if format is unknown or null
            }

            double error = predictedNumericValue - actualValue;
            sumSquaredError += error * error; // Accumulate squared error

        } else {
            // Attribute type is neither nominal nor numeric? Should not happen with standard MOA headers.
            System.err.println("PredictionProcessor[" + uniqueModelId + "]: Class attribute '" + header.classAttribute().name() + "' is neither Nominal nor Numeric.");
        }
    }


    /**
     * Logs throughput information to console and file at defined thresholds.
     */
    private void logThroughput(long threshold) {
        long now = System.currentTimeMillis();
        // Ensure start time is set
        if (batchProcessingStartMillis < 0) return;

        double elapsedSec = (now - batchProcessingStartMillis) / 1000.0;
        if (elapsedSec <= 0) elapsedSec = 0.001; // Prevent division by zero

        // Calculate TPS based on records processed *since the timer started*
        // Note: predictionRecordsProcessed is the *total* count for the instance.
        // For interval TPS, you might need a separate counter reset periodically.
        // This implementation logs TPS based on total processed / total time since first record in interval.
        double tps = predictionRecordsProcessed / elapsedSec;

        // Check thresholds against the *total* processed count
        // for (long threshold : THROUGHPUT_THRESHOLDS) {
        //  if (predictionRecordsProcessed >= threshold && !loggedThresholds.contains(threshold)) {
        String logMessage = String.format(
                "PREDICTION THRESHOLD %,d: Predicted %,d records in %.2f s (Avg %.1f tuples/sec since start) – algo=%s streamID=%s",
                threshold, predictionRecordsProcessed, elapsedSec, tps,
                algorithmType, streamID
        );
        writeThroughputToFile(logMessage);
        System.out.println(logMessage);
        loggedThresholds.add(threshold); // Mark as logged
        // }
        //  }

        // Optional: Reset timer periodically for interval-based TPS logging
        // This example resets every 1 million records for demonstration
        // if (predictionRecordsProcessed % 1_000_000 == 0 && predictionRecordsProcessed > 0) {
        //    System.out.println("PredictionProcessor[" + uniqueModelId + "]: Resetting throughput timer at " + predictionRecordsProcessed + " records.");
        //    batchProcessingStartMillis = System.currentTimeMillis(); // Reset timer start
        //    // If you want interval TPS, you'd reset a separate counter here:
        //    // intervalProcessedCount = 0;
        //    loggedThresholds.clear(); // Reset logged thresholds for new interval measurement
        // }
    }


    /**
     * Helper method to write a message to the throughput log file.
     */
    private void writeThroughputToFile(String message) {
        if (throughputWriter != null) {
            try {
                throughputWriter.write(message);
                throughputWriter.newLine();
                throughputWriter.flush(); // Ensure data is written promptly
            } catch (IOException e) {
                System.err.println("PredictionProcessor[" + uniqueModelId + "]: Error writing to throughput log file: " + e.getMessage());
                // Optionally handle error, e.g., stop trying to write
                // throughputWriter = null;
            }
        } else {
            // This might happen if init failed to open the file
            System.err.println("PredictionProcessor[" + uniqueModelId + "]: Throughput writer is not initialized. Cannot write log: " + message);
        }
    }

    /**
     * Convert DataStructure -> Instance (for Weka/MOA)
     * Overloaded version accepting pre-fetched header for efficiency.
     */
    private Instance convertToInstance(DataStructure data, MLAlgorithm model, InstancesHeader header) {
        // InstancesHeader header = getModelHeader(model); // Get header from model
        if (header == null) {
            System.err.println("PredictionProcessor[" + uniqueModelId + "]: Model has no InstancesHeader – cannot build instance");
            return null;
        }

        Map<String, Object> fieldsMap = data.getFields();
        if (fieldsMap == null) {
            System.err.println("PredictionProcessor[" + uniqueModelId + "]: DataStructure has null fields map – cannot build instance");
            return null;
        }
        double[] values = new double[header.numAttributes()];

        for (int attrIndex = 0; attrIndex < header.numAttributes(); attrIndex++) {
            Attribute attr = header.attribute(attrIndex);
            String attrName = attr.name();
            Object rawVal = fieldsMap.get(attrName); // Get value by attribute name

            if (attrIndex == header.classIndex()) {
                // For prediction, we usually set the class value as missing,
                // unless the actual value is present and needed for evaluation later
                // Let's check if the target value exists in the map
                if (fieldsMap.containsKey(target) && fieldsMap.get(target) != null) {
                    // Actual value is present, encode it (needed for metrics calculation)
                    Object actualRawVal = fieldsMap.get(target);
                    if(attr.isNominal()) {
                        values[attrIndex] = encodeNominalValue(attr, actualRawVal);
                    } else { // Assuming numeric for regression target
                        values[attrIndex] = parseNumericOrMissing(actualRawVal);
                    }
                    // Add a check if encoding failed (returned missing)
                    if (values[attrIndex] == Utils.missingValue() && actualRawVal != null) {
                        System.out.println("PredictionProcessor[" + uniqueModelId + "]: WARNING - Failed to encode non-null actual value '" + actualRawVal + "' for class attribute '" + attrName + "'. Instance class will be marked missing.");
                    }
                } else {
                    // Actual value is missing or not provided, mark as missing in instance
                    values[attrIndex] = Utils.missingValue();
                }

            } else {
                // It's a feature attribute
                if (attr.isNominal()) {
                    // If features can be nominal, handle them here properly
                    // This requires knowing the possible nominal values for features as well.
                    // For now, we still assume features are numeric or treat unparseable as missing.
                    // values[attrIndex] = encodeNominalValue(attr, rawVal); // Requires feature nominal defs
                    System.err.println("PredictionProcessor[" + uniqueModelId + "]: WARNING - Nominal feature attributes ('"+ attrName +"') not fully handled in convertToInstance. Assuming numeric or missing.");
                    values[attrIndex] = parseNumericOrMissing(rawVal); // Fallback for now
                } else { // Assuming numeric feature
                    values[attrIndex] = parseNumericOrMissing(rawVal);
                }
            }
        }

        // Create and set the instance's dataset (header)
        Instance inst = new DenseInstance(1.0, values); // Weight 1.0 by default
        inst.setDataset(header); // Link instance to header
        return inst;
    }


    /**
     * Helper to retrieve the InstancesHeader from different MLAlgorithm wrapper types.
     * Add cases here for any new MLAlgorithm wrapper classes you create.
     */
    private InstancesHeader getModelHeader(MLAlgorithm model) {
        // Use instanceof checks to call getInstancesHeader() method specific to your wrapper classes
        if (model instanceof MoaDecisionTreeModel) return ((MoaDecisionTreeModel) model).getInstancesHeader();
        if (model instanceof MoaNaiveBayesModel) return ((MoaNaiveBayesModel) model).getInstancesHeader();
        if (model instanceof MoaKNNModel) return ((MoaKNNModel) model).getInstancesHeader();
        if (model instanceof MoaRandomForestModel) return ((MoaRandomForestModel) model).getInstancesHeader();
        if (model instanceof MoaPerceptronModel) return ((MoaPerceptronModel) model).getInstancesHeader();
        if (model instanceof MoaHATModel) return ((MoaHATModel) model).getInstancesHeader();
        if (model instanceof MoaSGDModel) return ((MoaSGDModel) model).getInstancesHeader();
        if (model instanceof MoaFIMTDDModel) return ((MoaFIMTDDModel) model).getInstancesHeader();
        if (model instanceof MoaAMRulesRegressorModel) return ((MoaAMRulesRegressorModel) model).getInstancesHeader();
        if (model instanceof MoaRandomForestRegressorModel) return ((MoaRandomForestRegressorModel) model).getInstancesHeader();

        // Add other model types if necessary...

        System.err.println("PredictionProcessor[" + uniqueModelId + "]: Could not get InstancesHeader for model type: " + model.getClass().getName());
        return null; // Return null if header cannot be retrieved
    }


    private double parseNumericOrMissing(Object rawVal) {
        if (rawVal == null) {
            return Utils.missingValue(); // MOA/Weka representation for missing values
        }
        if (rawVal instanceof Number) {
            return ((Number) rawVal).doubleValue();
        }
        try {
            // Trim whitespace which might cause parse errors
            return Double.parseDouble(rawVal.toString().trim());
        } catch (NumberFormatException e) {
            // System.out.println("PredictionProcessor[" + uniqueModelId + "]: WARNING: Could not parse '" + rawVal + "' as number. Treating as missing.");
            return Utils.missingValue();
        } catch (NullPointerException e) {
            // Should be caught by rawVal == null check, but belt-and-suspenders
            return Utils.missingValue();
        }
    }

    private double encodeNominalValue(Attribute attr, Object rawLabel) {
        if (rawLabel == null) {
            return Utils.missingValue(); // Missing nominal value
        }
        // Convert rawLabel to String for lookup, handle potential null from toString()
        String labelStr = Objects.toString(rawLabel, null);
        if (labelStr == null) {
            System.out.println("PredictionProcessor[" + uniqueModelId + "]: WARNING: Received null after toString() for nominal value, attribute '" + attr.name() + "'. Treating as missing.");
            return Utils.missingValue();
        }

        // Trim whitespace from the label before lookup
        labelStr = labelStr.trim();

        int idx = attr.indexOfValue(labelStr); // Get the index of the nominal value
        if (idx == -1) {
            // Value not found in the attribute's defined nominal values
            System.out.println("PredictionProcessor[" + uniqueModelId + "]: WARNING: Unrecognized nominal value '" + labelStr + "' for attribute '" + attr.name() + "'. Known values: " + attr.enumerateValues() + ". Treating as missing."); // Warning
            return Utils.missingValue(); // Return missing if value is not recognized
        }
        return idx; // Return the numeric index of the nominal value
    }

    @Override
    public void close() {
        System.out.println("Closing PredictionProcessor[" + uniqueModelId + "]");

        // Process any remaining records in the buffer before closing
        // Note: This might run into issues if Kafka Streams shuts down abruptly.
        // Punctuation is generally more reliable for periodic flushing.
        if (predictionBuffer != null && !predictionBuffer.isEmpty()) {
            System.out.println("PredictionProcessor[" + uniqueModelId + "]: Processing " + predictionBuffer.size() + " remaining records in buffer during close()...");
            processBatch(); // Attempt to process final batch
        }


        // --- Log final metrics ---
        System.out.println("--- Final Prediction Metrics for " + uniqueModelId + " ---");
        System.out.println("Total Prediction Records Processed (by this instance): " + predictionRecordsProcessed);
        System.out.println("Total Predictions Evaluated (where actual value was present): " + totalPredictions);

        if (taskType.equalsIgnoreCase("classification")) {
            if (totalPredictions > 0) {
                double accuracy = (double) correctPredictions / totalPredictions * 100.0;
                System.out.printf("Classification Accuracy: %.2f%% (%d / %d)%n", accuracy, correctPredictions, totalPredictions);
            } else {
                System.out.println("Classification Accuracy: N/A (No predictions evaluated)");
            }
        } else if (taskType.equalsIgnoreCase("regression")) {
            if (regressionCount > 0) {
                double mse = sumSquaredError / regressionCount;
                double rmse = Math.sqrt(mse);
                System.out.printf("Regression MSE: %.4f%n", mse);
                System.out.printf("Regression RMSE: %.4f%n", rmse);
                System.out.println("Regression Count (predictions evaluated): " + regressionCount);
            } else {
                System.out.println("Regression Metrics: N/A (No predictions evaluated)");
            }
        } else {
            System.out.println("No metrics calculated (Task type: " + taskType + ")");
        }
        System.out.println("--------------------------------------------------");


        // --- Log final total throughput ---
        if (firstRecordTimeMs > 0 && predictionRecordsProcessed > 0) {
            long elapsedMs = System.currentTimeMillis() - firstRecordTimeMs;
            if (elapsedMs > 0) {
                double tps = predictionRecordsProcessed * 1000.0 / elapsedMs;
                String finalLogMessage = String.format(
                        "[FINAL PREDICTION THROUGHPUT] %s -> Total Processed: %,d records in %.2f s (Overall Avg %.1f records/sec)",
                        uniqueModelId, predictionRecordsProcessed, elapsedMs / 1000.0, tps
                );
                writeThroughputToFile(finalLogMessage);
                System.out.println(finalLogMessage);
            } else {
                String finalLogMessage = String.format(
                        "[FINAL PREDICTION THROUGHPUT] %s -> Total Processed: %,d records. Could not calculate duration/TPS.",
                        uniqueModelId, predictionRecordsProcessed
                );
                writeThroughputToFile(finalLogMessage);
                System.out.println(finalLogMessage);
            }
        } else {
            String finalLogMessage = String.format(
                    "[FINAL PREDICTION THROUGHPUT] %s -> %d records processed.",
                    uniqueModelId, predictionRecordsProcessed // Show 0 if none processed
            );
            writeThroughputToFile(finalLogMessage);
            System.out.println(finalLogMessage);
        }


        // --- Close the throughput writer ---
        if (throughputWriter != null) {
            try {
                throughputWriter.close();
                System.out.println("PredictionProcessor[" + uniqueModelId + "]: Closed throughput log file.");
            } catch (IOException e) {
                System.err.println("PredictionProcessor[" + uniqueModelId + "]: Error closing throughput log file: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
