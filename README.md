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
| 1 | HoeffdingTree | Classification | `gracePeriod`, `splitConfidence`, `tieThreshold` |
| 2 | Naive-Bayes | Classification | `RandomSeed` |
| 3 | kNN | Classification, Regression | `k`, `nearestNeighbourSearchOption`, `limitOption` |
| 4 | Random-Forest | Classification | `ensembleSize`, `numberOfJobs`, `treeLearner`, `mFeaturesPerTreeSize` |
| 5 | Perceptron | Classification, Regression | `learningRatio` |
| 6 | HoeffdingAdaptiveTree | Classification | `gracePeriod`, `splitConfidence`, `tieThreshold` |
| 7 | SGD | Classification, Regression | `learningRate`, `lossFunctionOption`, `lambdaRegularization` |
| 8 | FIMT-DD | Regression | `splitConfidence`, `gracePeriod`, `tieThreshold`, `learningRatio` |
| 9 | AMRules | Regression | `gracePeriod`, `splitConfidence`, `tieThreshold` |


