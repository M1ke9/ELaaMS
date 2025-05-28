package ProduceData;


import Configuration.EnvironmentConfiguration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

public class SingleInstanceProducer {

    //private static final String TRAINING_FOLDER   = "C:\\kafka_projects\\kafka_1\\Dataset\\TrainingData-98000";
   // private static final String PREDICTION_FOLDER = "C:\\kafka_projects\\kafka_1\\Dataset\\PredictionData-42000";

   // private static final String TRAINING_FOLDER   = "/home/mmarketakis/Dataset/TrainingData";
   // private static final String PREDICTION_FOLDER = "/home/mmarketakis/Dataset/PredictionData";

    private static final String TRAINING_FOLDER   = EnvironmentConfiguration.getFilePathForTrainingTopic();
    private static final String PREDICTION_FOLDER = EnvironmentConfiguration.getFilePathForPredictionTopic();


    public static void main(String[] args) {
        // ... your existing code to create and start threads ...


        // 1) Collect JSON files from TrainingData folder
        List<String> trainingFiles = listJsonFiles(TRAINING_FOLDER);

        // 2) Collect JSON files from PredictionData folder
        List<String> predictionFiles = listJsonFiles(PREDICTION_FOLDER);

        // 3) We create threads for both training + prediction
        List<Thread> threads = new ArrayList<>();

        // Spawn training threads
        for (int i = 0; i < trainingFiles.size(); i++) {
            String filePath = trainingFiles.get(i);
            Thread t = new Thread(new TrainingProducer11(filePath));
            threads.add(t);
            t.start();
        }


        /*
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }




         */


        // Spawn prediction threads
        for (int i = 0; i < predictionFiles.size(); i++) {
            String filePath = predictionFiles.get(i);
            Thread t = new Thread(new PredictionProducer11(filePath));
            threads.add(t);
            t.start();
        }

        // Wait for all threads to finish:
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // All data has been sent – close the shared producer:
        ProducerSingleton.close();

        System.out.println("\nAll training and prediction data production completed in parallel!");
    }

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
