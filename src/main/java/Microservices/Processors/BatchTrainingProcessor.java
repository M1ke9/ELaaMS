package Microservices.Processors;

import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import com.yahoo.labs.samoa.instances.*;
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
import moa.core.Utils;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.HashSet;      // Import for HashSet
import java.util.Set;          // Import for Set
import java.util.Arrays;       // Import for Arrays

public class BatchTrainingProcessor implements Processor<String, DataStructure, Void, Void> {

    private final String modelStoreName;
    private final String algorithmType;
    private final int algorithmId;
    private final String streamID;
    private final String target;
    private final String uniqueModelId;

    // ---------------------- Batching parameters -------------------------
    //private static final int BATCH_SIZE = 10000;  // Flush when buffer hits this size
    //private static final int FLUSH_INTERVAL_SEC = ; // Time-based flush

    private List<DataStructure> buffer;          // In-memory buffer for incoming training data
    private long trainingRecordsProcessed = 0;  // Just for logging

    long trainingStartMillis;

    private boolean started = false;     // Track if we've set trainingStartMillis yet

    private KeyValueStore<String, MLAlgorithm> modelStore;
    private ProcessorContext<Void, Void> context;

    private String taskType;

    // --- Throughput Logging to File ---
    private transient BufferedWriter throughputWriter; // Use transient as BufferedWriter is not serializable
   // private static final String THROUGHPUT_FILE_PATH = "/home/mmarketakis/Dataset/metrics/TrainingResults.txt"; // The target file path
   private static final String THROUGHPUT_FILE_PATH = EnvironmentConfiguration.getFilePathForThroughputTraining();
    // Define thresholds at which to log throughput
//    private static final long[] THROUGHPUT_THRESHOLDS = {100_000, 300_000, 500_000, 700_000, 880_000, 1_000_000};
    private static final long[] THROUGHPUT_THRESHOLDS = { 300_000, 490000,840000,1000000,16800000,1960000};
    private final Set<Long> loggedThresholds = new HashSet<>(); // Keep track of thresholds already logged

    // private static final long SAVE_INTERVAL_RECORDS = 50_000; // Not used in your provided code
    // private long nextSaveThreshold = SAVE_INTERVAL_RECORDS; // Not used
    // private static final String DEFAULT_MODEL_PATH = "/models/"; // Not used
    // public int  savecount=0; // Not used

    private long firstRecordTimeMs = -1;   // set on the very first tuple
    private long processed          = 0;   // Total records processed by this processor instance

    Map<String,Object> hyperParams;
    private final long duration; // Punctuation interval in milliseconds

    public BatchTrainingProcessor(Duration duration,String modelStoreName, String uniqueModelId, String algorithmType,
                                  int algorithmId, String streamID, String target,String taskType,Map<String,Object> hyperParams) {
        this.modelStoreName = modelStoreName;
        this.uniqueModelId = uniqueModelId;
        this.algorithmType = algorithmType;
        this.algorithmId = algorithmId;
        this.streamID = streamID;
        this.target = target;
        this.taskType=taskType;
        this.hyperParams    = (hyperParams != null) ? hyperParams : new HashMap<>();
        this.duration = duration.toMillis();
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        this.modelStore = context.getStateStore(modelStoreName);
        this.buffer = new ArrayList<>(); // Initialize the buffer

        // --- Initialize the throughput writer ---
        try {
            // Open the file in append mode (true)
            throughputWriter = new BufferedWriter(new FileWriter(THROUGHPUT_FILE_PATH, true));
            System.out.println("Opened throughput log file: " + THROUGHPUT_FILE_PATH);
        } catch (IOException e) {
            System.err.println("FATAL: Could not open throughput log file: " + THROUGHPUT_FILE_PATH);
            e.printStackTrace();
            // Depending on requirements, you might want to make this non-fatal or stop the stream
            // throw new org.apache.kafka.streams.errors.StreamsException("Failed to open throughput log file", e);
        }
        // --- End Initialization ---


        context.schedule(
                Duration.ofMillis(duration),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> flushBuffer()
        );
    }

    @Override
    public void process(Record<String, DataStructure> record) {
        if (firstRecordTimeMs < 0) {
            firstRecordTimeMs = System.currentTimeMillis();
        }
        processed++; // Increment total processed count for final log

        if (!started) {
            started = true;
            trainingStartMillis = System.currentTimeMillis(); // Start timer on first record
        }

        DataStructure dataInstance = record.value();
        if (dataInstance==null) {
            return; // Skip null records
        }
        buffer.add(record.value()); // Add to in-memory buffer

        // Optional: Add batch size flush logic here if needed
        // if (buffer.size() >= BATCH_SIZE) {
        //    flushBuffer();
        // }
    }

    /**
     * Flush buffer: retrieve (or create) the model, train on each instance in the batch, then store updated model.
     */
    private void flushBuffer() {
        if (buffer.isEmpty()) {
            return; // Nothing to process
        }

        // 1) Get or create the model from the state store
        MLAlgorithm model = modelStore.get(uniqueModelId);
        if (model == null) {
            System.out.println("CREATING MODEL FROM SCRATCH...");
            // Use the first record in the buffer to build an InstancesHeader
            DataStructure firstData = buffer.get(0);
            model = createModel(algorithmType, algorithmId, firstData);
            modelStore.put(uniqueModelId, model);

            if (model.getHyperParams() != null && !model.getHyperParams().isEmpty()) {
                System.out.println("Parameters for new " + algorithmType +  " model => " + model.getHyperParams());
            } else {
                System.out.println("No hyperparameters provided => using defaults.");
            }
        }


        // 2) Train on each record in the buffer
        for (DataStructure data : buffer) {
            Instance instance = convertToInstance(data, model);
            if (instance != null) {
                model.train(instance);
                trainingRecordsProcessed++; // Increment training count
            }
        }

        // 3) Store the updated model in the state store
        modelStore.put(uniqueModelId, model);

        // --- Log Throughput to File (and console) at defined thresholds ---
        long now = System.currentTimeMillis();
        double elapsedSec = (now - trainingStartMillis) / 1000.0;
        // Prevent division by zero if time delta is too small
        if (elapsedSec <= 0) elapsedSec = 0.001;

        double tps = trainingRecordsProcessed / elapsedSec;

        // Check if any defined threshold has been crossed since the last check and log it
        for (long threshold : THROUGHPUT_THRESHOLDS) {
            if (trainingRecordsProcessed >= threshold && !loggedThresholds.contains(threshold)) {
                String logMessage = String.format(
                        "TRAINING THRESHOLD %,d: Trained %,d records in %.2f s (%.1f tuples/sec) – algo=%s streamID=%s target=%s",
                        threshold, trainingRecordsProcessed, elapsedSec, tps,
                        algorithmType, streamID, target
                );
                writeThroughputToFile(logMessage); // Write to file
                System.out.println(logMessage); // Still print to console for real-time monitoring

                // Optionally log model info to console at these thresholds too
                // System.out.println("MODEL: " + uniqueModelId);

                loggedThresholds.add(threshold); // Mark this threshold as logged
            }
        }
        // --- End Throughput Logging in Flush ---

        // 4) General Logging (less detailed)
        if (trainingRecordsProcessed % 100_000 == 0 && !loggedThresholds.contains(trainingRecordsProcessed)) { // Avoid double logging if 100k is also a threshold
            System.out.println("BatchTrainingProcessor processed "
                    + trainingRecordsProcessed + " records total for (algorithm: "
                    + algorithmType + ", streamID: " + streamID + ", target: " + target + ")");
        }


        // 5) Clear the buffer for the next batch
        buffer.clear();

        // --- Save model logic (commented out in your original code) ---
        /*
        if (trainingRecordsProcessed >= nextSaveThreshold) {
            saveModelToDisk(model, uniqueModelId);
            nextSaveThreshold += SAVE_INTERVAL_RECORDS;
            savecount++;
            System.out.println("SAVE :" +savecount);
        }
        */
    }

    /**
     * Helper method to write a message to the throughput log file.
     */
    private void writeThroughputToFile(String message) {
        if (throughputWriter != null) {
            try {
                throughputWriter.write(message);
                throughputWriter.newLine(); // Add a newline after the message
                // Optional: flush immediately (can impact throughput slightly but ensures data is written)
                throughputWriter.flush();
            } catch (IOException e) {
                System.err.println("Error writing to throughput log file: " + e.getMessage());
                e.printStackTrace();
                // Optionally set writer to null to stop further write attempts after failure
                // throughputWriter = null;
            }
        } else {
            System.err.println("Throughput writer is not initialized. Cannot write log: " + message);
        }
    }

    /**
     * Create a fresh ML model with a new InstancesHeader if none found in the store.
     */
    private MLAlgorithm createModel(String algorithmType, int algorithmId, DataStructure firstData) {
        InstancesHeader header = buildHeaderFromFields(firstData);
        if (header == null) {
            // Handle error: Cannot create model without header
            throw new IllegalStateException("Cannot create model: InstancesHeader is null.");
        }


        if ("HoeffdingTree".equalsIgnoreCase(algorithmType) && algorithmId == 1) {
            return new MoaDecisionTreeModel(algorithmId, algorithmType, hyperParams, header);
        }
        else if ("Naive-Bayes".equalsIgnoreCase(algorithmType) && algorithmId == 2) {
            return new MoaNaiveBayesModel(algorithmId, algorithmType, hyperParams, header);
        }
        else if ("kNN".equalsIgnoreCase(algorithmType) && algorithmId == 3) {
            return new MoaKNNModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("Random-Forest".equalsIgnoreCase(algorithmType) && algorithmId == 4) {
            return new MoaRandomForestModel(algorithmId, algorithmType, hyperParams, header);
        }
        else if ("Perceptron".equalsIgnoreCase(algorithmType) && algorithmId == 5) {
            return new MoaPerceptronModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("HAT".equalsIgnoreCase(algorithmType) && algorithmId == 6) {
            return new MoaHATModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("SGD".equalsIgnoreCase(algorithmType) && algorithmId == 7) {
            return new MoaSGDModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("FIMT-DD".equalsIgnoreCase(algorithmType) && algorithmId == 8) {
            return new MoaFIMTDDModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("AMRules".equalsIgnoreCase(algorithmType) && algorithmId == 9) {
            return new MoaAMRulesRegressorModel(algorithmId, algorithmType, hyperParams, header);
        }

        else if ("Random-Forest-Regressor".equalsIgnoreCase(algorithmType) && algorithmId == 10) {
            return new MoaRandomForestRegressorModel(algorithmId, algorithmType, hyperParams, header);
        }

        throw new IllegalArgumentException("Unsupported algorithm type: " + algorithmType
                + ", ID: " + algorithmId + " for task: " + taskType); // Include taskType in error
    }

    /**
     * Build InstancesHeader from the fields in the DataStructure
     */
    private InstancesHeader buildHeaderFromFields(DataStructure data) {
        Map<String, Object> fields = data.getFields();
        if (fields == null || fields.isEmpty()) {
            System.err.println("No fields in data, cannot build InstancesHeader");
            return null; // Cannot build header
        }

        // Build a list of features from all fields except the target:
        List<String> featureNames = new ArrayList<>(fields.keySet());
        featureNames.remove(target); // Remove the target from feature list

        List<Attribute> attributes = new ArrayList<>();
        for (String feat : featureNames) {
            // Assuming all features are numeric based on your data sample
            // Add more sophisticated type handling if needed
            attributes.add(new Attribute(feat)); // numeric attribute
        }

        // Distinguish classification vs. regression for the class attribute:
        if (taskType.equalsIgnoreCase("classification") ) {
            // classification => nominal class attribute
            // Note: Using a dummy list for class values here as per your original code (algorithmId == 5 logic)
            // You might need to dynamically determine the actual class values from data or config
            List<String> classValues = new ArrayList<>();

            System.out.println("Building header for classification task.");
            Attribute classAttr = new Attribute(target, classValues);
            // System.out.println("IS CLASS ATR nominal? : " + classAttr.isNominal()); // Debug print

            attributes.add(classAttr); // Add class attribute
        } else if (taskType.equalsIgnoreCase("regression")) {
            // regression => numeric class attribute
            System.out.println("Building header for regression task.");
            Attribute classAttr = new Attribute(target);
            // System.out.println("IS CLASS ATR nominal? : " + classAttr.isNominal()); // Debug print

            attributes.add(classAttr); // Add class attribute
        } else {
            System.err.println("Unknown task type: " + taskType + ". Cannot build InstancesHeader.");
            return null; // Cannot build header for unknown task
        }


        InstancesHeader header = new InstancesHeader(new Instances("Data", attributes, 0));
        // Set the class index to the last attribute added (the target)
        header.setClassIndex(attributes.size() - 1);

        System.out.println(" TRAINING Header built: Number of Attributes = " + header.numAttributes()+ ", Class Index = " + header.classIndex() + " (" + header.attribute(header.classIndex()).name() + ")");

        return header;
    }

    /**
     * Convert DataStructure -> Instance (for Weka/MOA)
     */
    private Instance convertToInstance(DataStructure data, MLAlgorithm model) {
        InstancesHeader header = getModelHeader(model); // Helper to get header
        if (header == null) {
            System.err.println("Model has no InstancesHeader – cannot build instance");
            return null;
        }

        Map<String, Object> fieldsMap = data.getFields();
        double[] values = new double[header.numAttributes()];

        for (int attrIndex = 0; attrIndex < header.numAttributes(); attrIndex++) {
            Attribute attr = header.attribute(attrIndex);
            String attrName = attr.name();
            Object rawVal = fieldsMap.get(attrName); // Get value by attribute name

            if (attrIndex == header.classIndex()) {
                // It's the target attribute
                if(attr.isNominal()) {
                    values[attrIndex] = encodeNominalValue(attr, rawVal);
                } else {
                    // Assuming numeric for regression target
                    values[attrIndex] = parseNumericOrMissing(rawVal);
                }
            } else {
                // It's a feature attribute (assuming numeric based on your data sample)
                values[attrIndex] = parseNumericOrMissing(rawVal);
            }
        }

        // Create and set the instance's dataset (header)
        Instance inst = new DenseInstance(1.0, values); // Weight 1.0 by default
        inst.setDataset(header); // Link instance to header
        return inst;
    }

    /**
     * Helper to retrieve the InstancesHeader from different MLAlgorithm wrapper types.
     * Add cases here for any new MLAlgorithm wrapper classes you create that have a getInstancesHeader() method.
     */
    private InstancesHeader getModelHeader(MLAlgorithm model) {
        // Use instanceof checks to call getInstancesHeader() method specific to your wrapper classes
        // This is necessary if MLAlgorithm is an interface or base class without this method defined
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

        // If MLAlgorithm base class has getInstancesHeader(), call it:
        // if (model != null) return model.getInstancesHeader(); // Uncomment if getInstancesHeader is in MLAlgorithm

        return null; // Return null if header cannot be retrieved
    }


    private double parseNumericOrMissing(Object rawVal) {
        if (rawVal == null) {
            // System.out.println("TRAINING NULL or MISSING VALUE"); // Optional debug
            return Utils.missingValue(); // MOA/Weka representation for missing values
        }
        if (rawVal instanceof Number) {
            return ((Number) rawVal).doubleValue();
        }
        try {
            // Attempt to parse String representation of a number
            return Double.parseDouble(rawVal.toString());
        } catch (NumberFormatException e) {
            // System.out.println("WARNING: Could not parse '" + rawVal + "' as number. Treating as missing."); // Optional debug
            return Utils.missingValue(); // Return missing value if parsing fails
        }
    }

    private double encodeNominalValue(Attribute classAttr, Object rawLabel) {
        if (rawLabel == null) {
            return Utils.missingValue(); // Missing nominal value
        }
        String labelStr = rawLabel.toString();
        int idx = classAttr.indexOfValue(labelStr); // Get the index of the nominal value
        if (idx == -1) {
            // Value not found in the attribute's defined nominal values
            System.out.println("WARNING: Unrecognized nominal value '" + labelStr + "' for attribute '" + classAttr.name() + "'. Treating as missing."); // Warning
            return Utils.missingValue(); // Return missing if value is not recognized
        }
        return idx; // Return the numeric index of the nominal value
    }

    // Method to save model to disk (commented out in your original code)

    private void saveModelToDisk(MLAlgorithm model, String modelId) {

        String subpath = EnvironmentConfiguration.getFilePathPrefix();
        String path = subpath + modelId + ".bin"; // Example path

         try (FileOutputStream fos = new FileOutputStream(path);
              ObjectOutputStream oos = new ObjectOutputStream(fos)) {
             oos.writeObject(model); // Write the serializable model object
             oos.flush();
             System.out.println("Saved model to disk at " + path);
         } catch (IOException e) {
             e.printStackTrace();
             System.err.println("Failed to save model to disk: " + e.getMessage());
         }
    }


    @Override
    public void close() {
        // --- Log final total throughput ---
        long elapsedMs = System.currentTimeMillis() - firstRecordTimeMs;
        if (elapsedMs > 0 && processed > 0) { // Ensure time elapsed and records processed
            double tps = processed * 1000.0 / elapsedMs;
            String finalLogMessage = String.format(
                    "[FINAL THROUGHPUT] %s -> Total Processed: %,d records in %.2f s (Average %.1f records/sec)",
                    uniqueModelId, processed, elapsedMs / 1000.0, tps
            );
            writeThroughputToFile(finalLogMessage); // Write to file
            System.out.println(finalLogMessage); // Still print to console
        } else if (processed == 0) {
            // Log that no records were processed if that's the case
            String finalLogMessage = String.format(
                    "[FINAL THROUGHPUT] %s -> 0 records processed.",
                    uniqueModelId
            );
            writeThroughputToFile(finalLogMessage); // Write to file
            System.out.println(finalLogMessage); // Still print to console
        } else {
            // Log if elapsedMs is not > 0 but processed > 0 (shouldn't happen with firstRecordTimeMs logic)
            String finalLogMessage = String.format(
                    "[FINAL THROUGHPUT] %s -> Total Processed: %,d records. Could not calculate duration/TPS.",
                    uniqueModelId, processed
            );
            writeThroughputToFile(finalLogMessage);
            System.out.println(finalLogMessage);
        }


        // --- Close the throughput writer ---
        if (throughputWriter != null) {
            try {
                throughputWriter.close(); // Close the file writer
                System.out.println("Closed throughput log file: " + THROUGHPUT_FILE_PATH);
            } catch (IOException e) {
                System.err.println("Error closing throughput log file: " + e.getMessage());
                e.printStackTrace();
            }
        }
        // --- End Close ---

        // Ensure we flush any remaining records at shutdown - commented out as flushBuffer is scheduled by time
        // flushBuffer(); // This might not be needed here if flushBuffer is handled by punctuation
    }
}