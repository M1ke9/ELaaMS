package ConvertBigData;

import Configuration.EnvironmentConfiguration;
import Structure.DataStructure;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class ConvertToJsonTraining {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Converts a single CSV/Log file to line-delimited JSON of DataStructure objects,
     * attempting to infer and set proper data types (Integer, Double, Boolean)
     * for fields where applicable.
     * File name must be e.g. "streamId-datasetKey.csv" or "streamId-datasetKey.logs"
     * so that streamId="streamId", datasetKey="datasetKey".
     * This method is now intended as a helper for convertFolderToJson.
     */
    private static void convertSingleFileToJson(String filePath) throws IOException {
        File inputFile = new File(filePath);
        String fileName = inputFile.getName(); // e.g. cargoSHIPS-SHIPS.logs
        String baseName;

        // Extract base name without extension. Handles cases with or without a dot.
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < fileName.length() - 1) {
            baseName = fileName.substring(0, dotIndex);
        } else {
            // No extension or dot is at the very beginning/end
            baseName = fileName;
        }

        String[] parts = baseName.split("-");
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "File name '" + fileName + "' must be in the format 'streamId-datasetKey.ext'"
            );
        }
        String streamId = parts[0];
        String datasetKey = parts[1];

        // Output JSON file will have the same baseName, but with .json
        String parentPath = (inputFile.getParent() == null) ? "." : inputFile.getParent();
        String outputJsonPath = Paths.get(parentPath, baseName + ".json").toString();


        System.out.println("Converting '" + fileName + "' to JSON...");

        try (
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8)
                );
                BufferedWriter bw = new BufferedWriter(new FileWriter(outputJsonPath))
        ) {
            // read header line
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                System.out.println("Warning: CSV/Log file is empty or has no header: " + filePath + ". Skipping.");
                return; // Skip empty files
            }
            String[] headers = headerLine.split(",");

            // For each data line, create a DataStructure
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", -1); // -1 to capture empty fields

                // Build fields map
                Map<String, Object> fieldsMap = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String colName = headers[i].trim();
                    String colValue = (i < values.length) ? values[i].trim() : "";

                    // Attempt to infer and set the correct type
                    Object typedValue = inferType(colValue);
                    fieldsMap.put(colName, typedValue);
                }
                DataStructure ds = new DataStructure();
                ds.setStreamID(streamId);
                ds.setDataSetKey(datasetKey);

                // IMPORTANT: RECORD ID IS NOT NEEDED FOR TRAINING DATA ( EMPTY STRING)
                ds.setRecordID("");
                ds.setFields(fieldsMap);

                // Serialize to JSON
                String jsonLine = objectMapper.writeValueAsString(ds);
                bw.write(jsonLine);
                bw.newLine();
            }
        }
        System.out.println("Successfully created JSON file: " + outputJsonPath);
    }

    /**
     * Converts all eligible CSV/Log files within a specified folder to line-delimited JSON.
     * Eligible files must end with '.csv' or '.logs' and follow the 'streamId-datasetKey.ext' naming convention.
     *
     * @param folderPath The path to the folder containing the CSV/Log files.
     */
    public static void convertFolderToJson(String folderPath) {
        System.out.println("Starting conversion of files in folder: " + folderPath);
        Path dir = Paths.get(folderPath);

        if (!Files.exists(dir)) {
            System.err.println("Error: Directory does not exist: " + folderPath);
            return;
        }
        if (!Files.isDirectory(dir)) {
            System.err.println("Error: Path is not a directory: " + folderPath);
            return;
        }

        long filesConverted = 0;
        try (Stream<Path> paths = Files.walk(dir)) {
            List<Path> eligibleFiles = paths
                    .filter(Files::isRegularFile) // Only consider regular files
                    .filter(p -> p.toString().toLowerCase().endsWith(".csv") || p.toString().toLowerCase().endsWith(".logs")) // Filter for .csv or .logs
                    .toList(); // Collect to a list to avoid issues with stream closure if an exception occurs during conversion

            if (eligibleFiles.isEmpty()) {
                System.out.println("No .csv or .logs files found in folder: " + folderPath);
                return;
            }

            for (Path filePath : eligibleFiles) {
                try {
                    convertSingleFileToJson(filePath.toString());
                    filesConverted++;
                } catch (IllegalArgumentException e) {
                    System.err.println("Skipping file due to naming convention error: " + filePath.getFileName() + " - " + e.getMessage());
                } catch (IOException e) {
                    System.err.println("Error converting file " + filePath.getFileName() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.err.println("Error walking directory " + folderPath + ": " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Completed conversion for folder: " + folderPath + ". Converted " + filesConverted + " files.");
    }

    /**
     * Attempts to infer the data type of a string value.
     * Tries to parse as Integer, then Double, then Boolean. Defaults to String if none match.
     */
    private static Object inferType(String value) {
        if (value == null || value.isEmpty()) {
            return null; // Represent empty or null as JSON null
        }

        // Try as Integer
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            // Not an Integer, try as Double
        }

        // Try as Double
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            // Not a Double, try as Boolean
        }

        // Try as Boolean (case-insensitive)
        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(value)) {
            return Boolean.FALSE;
        }

        // Default to String if no other type matches
        return value;
    }

    public static void main(String[] args) throws IOException {
        // --- Option 1: Convert a single file (for testing individual files) ---
        // String singleFilePath = "C:\\kafka_projects\\kafka_1\\DataExamples\\TrainingData\\AegeanShips-Ships.logs";
        // try {
        //     convertSingleFileToJson(singleFilePath);
        // } catch (Exception e) {
        //     e.printStackTrace();
        // }


        // --- Option 2: Convert all files in a folder (Recommended for batch processing) ---
        // IMPORTANT: Replace this with the actual path to your folder containing .csv or .logs files
//String inputFolderPath = "C:\\kafka_projects\\kafka_1\\DataExamples\\Regression\\TrainingData";
        // You could also get this from a configuration if you set it up:
        // String inputFolderPath = EnvironmentConfiguration.getFilePathForTrainingTopic(); // Assuming this returns a folder path
        String inputFolderPath = EnvironmentConfiguration.getFilePathForTrainingTopic();
        convertFolderToJson(inputFolderPath);
    }
}