# An Ensemble Learning Engine with Kafka And Kafka Streams Microservices

## Abstract
This work introduces ELaaMS (Ensemble Learning as a MicroService), an event-driven
microservice architecture designed to deliver robust, real-time ensemble predictions from
multiple streaming machine-learning models running concurrently, built on top of Apache
Kafka and Kafka Streams. Within ELaaMS, each learner runs as an independent Kafka
Streams application that consumes streaming data, trains incrementally with the Massive
Online Analysis (MOA) library, and publishes live predictions. A lightweight ensemble
aggregator service fuses these outputs on the fly—applying majority voting for classification and simple averaging for regression tasks —while a built-in catalog of streaming
machine learning algorithms makes the system usable out of the box and readily extensible through the integration of new algorithms and methods. This architecture is designed
to support robust scalability. Horizontal scalability is achieved by leveraging Kafka’s capability to distribute the workload across additional instances of the application, thereby
increasing overall processing capacity and throughput. Vertical scalability is supported
by allocating more computational resources (e.g., CPU, memory, threads) to individual
ELaaMS component instances, enabling them to handle increased data throughput or
more complex processing tasks.

## Configuration ELaaMS before execution:
1. For the application to function properly, the following parameters must be set in the `config.properties` file.
   Following is an example of the `config.properties` file as must implement in case of running the application in a Windows environment.

```properties
# Determine the Kafka Broker
BOOTSTRAP_SERVERS = localhost:29092,localhost:39092,localhost:49092

# Determine the path for saving Kafka Streams files
KAFKA_STREAM_DIR = C:/Dataset/tmp/kafka-streams

SAVE_FILE_PATH_PREFIX = C:/Dataset/Stored/
# Determine the number of parallel threads in Router Microservice
PARALLEL_DEGREE = 8

# Determine the replication factor of Kafka topics
REPLICATION_FACTOR = 3

# Determine the time (milliseconds) for batching messages of data to add this in the machine learning model.
TrainingBatchTime = 350
PredictionBatchTime = 350

TRAINING_TOPIC_PATH = C:/Dataset/TrainingData
PREDICTION_TOPIC_PATH = C:/Dataset/PredictionData
REQUEST_TOPIC_PATH = C:/Dataset/ControlRequests
THROUGHPUT_TRAIN = C:/Dataset/MetricsResults/TrainingResults.txt
THROUGHPUT_PREDICT = C:/Dataset/MetricsResults/PredictionResults.txt
```

2. Following is an example of the config.properties file as must implement in case of running the application in a Linux environment.
```properties
# Determine the Kafka Broker
BOOTSTRAP_SERVERS = localhost:29092,localhost:39092,localhost:49092

# Determine the path for saving Kafka Streams files
KAFKA_STREAM_DIR = /tmp/kafka-streams

SAVE_FILE_PATH_PREFIX = /home/Dataset/Stored/
# Determine the number of parallel threads in Router Microservice
PARALLEL_DEGREE = 8

# Determine the replication factor of Kafka topics
REPLICATION_FACTOR = 3

# Determine the time (milliseconds) for batching messages of data to add this in the machine learning model.
TrainingBatchTime = 350
PredictionBatchTime = 350
# Determine the REQUEST_PATH and DATA_PATH
TRAINING_TOPIC_PATH = /home/user/Dataset/TrainingData
PREDICTION_TOPIC_PATH = /home/user/Dataset/PredictionData
REQUEST_TOPIC_PATH = /home/user/Dataset/ControlRequests
THROUGHPUT_TRAIN = /home/user/Dataset/metrics/TrainingResults.txt
THROUGHPUT_PREDICT = /home/user/Dataset/metrics/PredictionResults.txt
```
3. For deployments involving the entire application via Docker, the `BOOTSTRAP_SERVERS` parameter must be correctly configured to ensure proper inter-container communication. Based on the provided `docker-compose.yml`, application containers should use the internal service names of the Kafka brokers.

```properties
# Determine the Kafka Broker (for Dockerized applications)
BOOTSTRAP_SERVERS = broker1:9092,broker2:9092,broker3:9092
```
4. Note that the Docker containers' volume mappings currently utilize Windows-specific host paths. These paths require modification when the application is deployed on a Linux Docker host.
5. In case of running the application in a cluster environment, the config.properties file must be set in all the nodes of the cluster.

6. In case of config.properties file is not found, the application will use default values as in config.properties file.

### Summary of Machine Learning Algorithms Implemented in ELaaMS

| ID | Algorithm Type | Task Type(s) | Key Hyperparameters |
|:---|:---------------|:-------------|:--------------------|
| 1 | HoeffdingTree  | Classification | `gracePeriod`, `splitConfidence`, `tieThreshold` |
| 2 | Naive-Bayes    | Classification | `RandomSeed` |
| 3 | kNN            | Classification, Regression | `k`, `nearestNeighbourSearchOption`, `limitOption` |
| 4 | Random-Forest  | Classification | `ensembleSize`, `numberOfJobs`, `treeLearner`, `mFeaturesPerTreeSize` |
| 5 | Perceptron     | Classification, Regression | `learningRatio` |
| 6 | HAT            | Classification | `gracePeriod`, `splitConfidence`, `tieThreshold` |
| 7 | SGD            | Classification, Regression | `learningRate`, `lossFunctionOption`, `lambdaRegularization` |
| 8 | FIMT-DD        | Regression | `splitConfidence`, `gracePeriod`, `tieThreshold`, `learningRatio` |
| 9 | AMRules        | Regression | `gracePeriod`, `splitConfidence`, `tieThreshold` |


## User Interface Application

## System Interaction: Control Requests

The ELaaMS application provides a user-friendly environment designed for accessible operation without requiring specialized skills. Interaction with the system is primarily conducted through structured JSON Requests. These requests define the specific actions and data involved in each system operation.

The structure of these JSON Requests includes the following standard fields:

* **`commandType`** (*String*): Specifies the type of operation requested from the system (e.g., `Create`, `Delete`, `Load` an algorithm instance).
* **`algorithmID`** (*Integer*): A unique numerical identifier assigned to the specific machine learning algorithm to be used for the task.
* **`algorithmType`** (*String*): Indicates the general category or name of the chosen algorithm implementation.
* **`streamID`** (*String*): Identifies the particular data stream instance that the deployed algorithm microservice should consume data from (e.g., a stock name like `EURTRY`).
* **`dataSetKey`** (*String*): Specifies the source from which the `streamID` originates (e.g., `Forex` for a stock market dataset).
* **`hyperParams`** (*Object*): A nested JSON object functioning as a key-value map to configure the specific hyperparameters required by the selected algorithm.
* **`target`** (*String*): Defines the name of the variable within the data stream that the algorithm is intended to predict (e.g., `'price'`).
* **`taskType`** (*String*): Specifies whether the requested machine learning task is `Classification` or `Regression`.

**Example Create Request:**

To deploy a microservice running a FIMT-DD (Fast Incremental Model Trees with Drift Detection) algorithm configured for `EURTRY` stock data from the `Forex` market, specifically targeting the `'price'` variable, the following JSON request would be used:

```json
{
  "commandType": "Create",
  "algorithmID": 8,
  "algorithmType": "FIMT-DD",
  "streamID": "EURTRY",
  "dataSetKey": "Forex",
  "hyperParams": {
    "gracePeriod": 500,
    "splitConfidence": 0.05,
    "learningRatioO": 0.001,
    "tieThresholdOption": 0.2
  },
  "target": "price",
  "taskType": "Regression"
}
```
**Example Delete Request:**

The following command stops and deletes from the system an existing ML model microservice which is currently running.
In the example below we demonstrate how a kNN microservice is stopped within ELaaMS.
It is  important to configure all other fields of the request(used in the creation), cause it could be more than one kNN microservices running (eg., with different parameters, on different target on that specific data scope or even on different data scope )
```json
{
  "commandType": "Delete",
  "algorithmID": 3,
  "algorithmType": "kNN",
  "streamID": "AegeanShips",
  "dataSetKey": "Ships",
  "hyperParams": {
    "k": 5
  },
  "target": "status",
  "taskType": "Classification"
}
```

## Data Tuple Structure

The data tuples processed by ELaaMS are structured to accommodate various datasets while maintaining a consistent format. Each tuple contains the following key attributes:

* **`streamID`** (*String*): A unique identifier specifying the particular data stream instance from which the data originates.
* **`dataSetKey`** (*String*): Represents the source or dataset to which the `streamID` belongs.
* **`recordID`** (*String*): A unique identifier for the individual data record represented by the tuple.
* **`fields`** (*Map<String, Object>*): A flexible key-value map containing all the feature attributes of the data instance. All features provided in the dataset are included here. The deployed machine learning models dynamically identify relevant features and the designated target variable based on the initial request configuration.

**Note on `recordID`:** For training data tuples, the `recordID` field is optional and can be left as an empty string. However, for prediction data specifically intended for ensemble grouping, the `recordID` field is mandatory and must contain a unique identifier (typically a UUID).

### Example Training Data Tuple:
For a dataset with features like `price`, `day`, `hour`, `minute`, `second`, `dayofweek`, a corresponding training data tuple would be represented as:

```json
{
  "streamID": "EURTRY",
  "dataSetKey": "Forex",
  "recordID": "",
  "fields": {
    "price": 6.24146,
    "day": 11,
    "hour": 0,
    "minute": 0,
    "second": 1,
    "dayofweek": 4
  }
}
```
## Data Producers

The ELaaMS application provides flexibility in data ingestion by offering two distinct types of producers, designed to handle various deployment and performance needs:

* **Single Instance, Multi-threaded Producer:** This producer type is suitable for scenarios where a single application instance generates data, utilizing multiple internal threads to achieve concurrent message production.
* **Multi-Instance, Multi-threaded Producer:** This advanced producer configuration is ideal for highly distributed environments. It allows multiple application instances, each potentially running with its own multi-threaded producer, to generate data in parallel, maximizing throughput and scalability.

## Ensemble Output Structure

For a comprehensive view of ensemble results, each output tuple is published to a dedicated Kafka topic. The topic name varies based on the **data scope** for a particular ensemble task:

* **Single-stream ensemble:** `OutputTopicForData-<streamID>-<dataSetKey>-<target>`
* **Multi-stream/dataset ensemble:** `OutputTopicForData-<dataSetKey>-<target>`

Each ensemble output tuple provides a detailed overview of the aggregated prediction and individual model contributions:

* **`recordId`** (*String*): A unique identifier for the data record.
* **`finalPrediction`** (*String*): The final, aggregated prediction from the ensemble (e.g., through majority voting or averaging).
* **`partialPredictions`** (*Array of Objects*): A list containing individual predictions from each contributing ML microservice. Each object in this array includes:
   * `algorithmType` (*String*): The type of algorithm that made the prediction.
   * `prediction` (*String*): The prediction value from that specific algorithm.
   * `hyperParams` (*String*): A string representation of the hyperparameters used by that algorithm for its prediction.

### Example Ensemble Output Tuple:

Below is an illustration of an ensemble tuple from a multi-class classification task involving four distinct ML microservices:

```json
{
  "recordId": "c0f98c53-b7bc-4abb-bfa3-e00cc6d91067",
  "finalPrediction": "13",
  "partialPredictions": [
    {
      "algorithmType": "HoeffdingTree",
      "prediction": "13",
      "hyperParams": "{gracePeriod=200, splitConfidence=0.01, tieThresholdOption=0.05}"
    },
    {
      "algorithmType": "kNN",
      "prediction": "7",
      "hyperParams": "{k=5}"
    },
    {
      "algorithmType": "kNN",
      "prediction": "11",
      "hyperParams": "{k=10}"
    },
    {
      "algorithmType": "Random-Forest",
      "prediction": "13",
      "hyperParams": "{ensembleSize=3}"
    }
  ]
}
```
## Getting Started
### Prerequisites

* **Java Development Kit (JDK):** Version 17
* **Apache Maven:** Version 3.9.6 
* **Apache Kafka & Kafka Streams:** Version 3.7.1
* **Git:** For cloning the repository
* **Docker & Docker Compose:** For running the Kafka cluster

The project was developed and tested on the following versions, but it could also work on other compatible versions.


## How to Execute ELaaMS:

### Step 1: Clone the Repository


First, clone the project repository to your local machine:

```bash
git clone https://github.com/M1ke9/ELaaMS.git
```

### Step 2: Build the Project
```bash
mvn clean package
```
### Step 3: Start the Kafka Cluster
```bash
docker-compose up -d broker1 broke2 broker3
```
### Step 4: Create Kafka Topics

The ELaaMS application requires several Kafka topics for its operation. You have two options to create them:

#### Option 1: Using the `kafka-setup` Docker Service (Recommended)

The `docker-compose.yml` includes a dedicated `kafka-setup` service designed to automatically create all required input topics when it starts. This is the simplest and recommended method.

```bash
docker-compose up -d kafka-setup
```
#### Option 2: Manually from the Command Line

If you prefer to create the topics manually (e.g., for debugging or specific configurations), you can execute the `kafka-topics.sh` commands directly within one of the Kafka broker containers.

1.  Access the shell of a Kafka broker container.

    ```bash
    docker exec --workdir /opt/kafka/bin/ -it broker1 sh
    ```

2.  Once inside the container's shell, run the following commands to create each topic:

    ```bash
    ./kafka-topics.sh --bootstrap-server localhost:29092 --create --topic control-topic --partitions 8 --replication-factor 3
    ./kafka-topics.sh --bootstrap-server localhost:29092 --create --topic training-topic --partitions 8 --replication-factor 3
    ./kafka-topics.sh --bootstrap-server localhost:29092 --create --topic prediction-topic --partitions 8 --replication-factor 3
    ./kafka-topics.sh --bootstrap-server localhost:29092 --create --topic active-microservices --partitions 1 --replication-factor 3
    ./kafka-topics.sh --bootstrap-server localhost:29092 --create --topic model-updates-topic --partitions 1 --replication-factor 3
    ```
### Step 5: Run the ELaaMS JAR

Once the Kafka cluster is running, you can start the ELaaMS microservices and producers. Ensure your configuration files (e.g., `config.properties`) are placed in a `Configuration/` directory at the project root.

#### Router Microservice

* **Windows:**
    ```bash
    java -DconfigFilePath=.\Configuration\config.properties -jar target/ELaaMS-app-jar-with-dependencies.jar
    ```
* **Linux/macOS:**
    ```bash
    java -DconfigFilePath=./Configuration/config.properties -jar target/ELaaMS-app-jar-with-dependencies.jar
    ```
The -DconfigFilePath isn't the exact path of the config.properties file, but it is an example. The user can set the path of the config.properties file according to the location of the file in the computer