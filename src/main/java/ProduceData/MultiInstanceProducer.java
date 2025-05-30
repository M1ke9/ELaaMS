package ProduceData;

import Configuration.EnvironmentConfiguration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * ProduceData1 enumerates JSON files for training and prediction,
 * spawns one thread per file, for both training and prediction,
 * and then waits for all threads at the end.
 *
 * This ensures training files and prediction files are produced in parallel.
 */
public class MultiInstanceProducer {

    // Number of partitions in "training-topic" & "prediction-topic"
    private static final int NUM_PARTITIONS = 4;

    // Folder paths:
   // private static final String TRAINING_FOLDER   = "C:\\kafka_projects\\kafka_1\\Dataset\\TrainingData";
   // private static final String PREDICTION_FOLDER = "C:\\kafka_projects\\kafka_1\\Dataset\\PredictionData";
    private static final String TRAINING_FOLDER   = EnvironmentConfiguration.getFilePathForTrainingTopic();
    private static final String PREDICTION_FOLDER = EnvironmentConfiguration.getFilePathForPredictionTopic();

    public static void main(String[] args) {
        // 1) Collect JSON files from TrainingData folder
        List<String> trainingFiles = listJsonFiles(TRAINING_FOLDER);

        // 2) Collect JSON files from PredictionData folder
        List<String> predictionFiles = listJsonFiles(PREDICTION_FOLDER);

        // 3) We create threads for both training + prediction
        List<Thread> threads = new ArrayList<>();

        // Spawn training threads
        for (int i = 0; i < trainingFiles.size(); i++) {
            String filePath = trainingFiles.get(i);
           // int partition = i % NUM_PARTITIONS; // simple partition assignment
            Thread t = new Thread(new TrainingProducer1(filePath));
            threads.add(t);
            t.start();
        }

        // Spawn prediction threads
        for (int i = 0; i < predictionFiles.size(); i++) {
            String filePath = predictionFiles.get(i);
           // int partition = i % NUM_PARTITIONS;
            Thread t = new Thread(new PredictionProducer1(filePath));
            threads.add(t);
            t.start();
        }

        // 4) Wait for all threads (training + prediction) to finish
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("\nAll training and prediction data production completed in parallel!");
    }

    // Utility to list all .json files in a given folder
    private static List<String> listJsonFiles(String folderPath) {
        List<String> result = new ArrayList<>();
        File folder = new File(folderPath);
        if (folder.isDirectory()) {
            FilenameFilter jsonFilter = (dir, name) -> name.endsWith(".json");
            File[] files = folder.listFiles(jsonFilter);
            if (files != null) {
                for (File file : files) {
                    result.add(file.getAbsolutePath());
                }
            }
        }
        return result;
    }
}

