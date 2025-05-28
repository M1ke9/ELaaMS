package Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;




public class EnvironmentConfiguration {
    private static final Properties properties = new Properties();
    private static final String Default_Location_DIR_PATH = "C:\\kafka_projects\\kafka_1\\Configuration\\configWindows.properties";

    static {
        String final_Location_Path = System.getProperty("configFilePath");

        if (final_Location_Path == null) {
            final_Location_Path = Default_Location_DIR_PATH;
        }

        try (InputStream input = new FileInputStream(final_Location_Path)) {
            System.out.println("Reading Configuration File...");
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            // Handle exception, e.g., by falling back to default values or terminating the application.
        }
    }





    public static String getBootstrapServers() {
        return properties.getProperty("BOOTSTRAP_SERVERS", "localhost:29092,localhost:39092,localhost:49092");  // Default value is used if property is not found
        //return  properties.getProperty("BOOTSTRAP_SERVERS", "broker:9092");
    }

    public static String getFilePathPrefix() {
        return properties.getProperty("SAVE_FILE_PATH_PREFIX","C:/dataset/Stored/");
    }


    public static String getTempDir() {
        return properties.getProperty("KAFKA_STREAM_DIR","C:/dataset/tmp/kafka-streams");
        //return properties.getProperty("KAFKA_STREAM_DIR","/tmp/kafka-streams");
    }

    public static int giveTheParallelDegree() {
        return Integer.parseInt(properties.getProperty("PARALLEL_DEGREE","8"));
    }

    public static int giveTheReplicationFactor() {
        return Integer.parseInt(properties.getProperty("REPLICATION_FACTOR","3"));
    }


    public static String getFilePathForTrainingTopic() {
        return properties.getProperty("TRAINING_TOPIC_PATH","C:/Dataset/TrainingData");
    }
    public static String getFilePathForPredictionTopic() {
        return properties.getProperty("PREDICTION_TOPIC_PATH","C:/Dataset/PredictionData");
    }

    public static String getFilePathForControlTopic() {
        return properties.getProperty("REQUEST_TOPIC_PATH","C:/Dataset/ControlRequests");
    }

    public static String getFilePathForThroughputTraining() {
        return properties.getProperty("THROUGHPUT_TRAIN", "C:/Dataset/MetricsResults/TrainingResults.txt");
    }

    public static String getFilePathForThroughputPredict() {
        return properties.getProperty("THROUGHPUT_PREDICT", "C:/Dataset/MetricsResults/PredictionResults.txt");
    }

    public static String getTrainingBatchTime() {
        return properties.getProperty("TrainingBatchTime","350");
    }

    public static String getPredictionBatchTime() {
        return properties.getProperty("PredictionBatchTime","350");
    }

}