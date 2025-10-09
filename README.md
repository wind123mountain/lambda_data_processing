# lambda-kappa-architecture

## Requirements

- Docker on your OS
- Optional: Atleast 16GB RAM, but can be set up in lower scale.

## Lambda

#### Start
```
docker-compose up -d # wait until all container are running completly (around 2 Minutes)
python start_lambda.py
```

After all containers are running we need to configure and start few things. 
Within the folder \lambda\src we can find few python files. Those files are necessary 
to generate data and tell Kafka, Spark, Hadoop and Cassandra what to do. 

![lambda-architecture](./docs/img/lambda-architecture.svg)
Figure 1: Lambda-Architecture

Whole lambda architecture is created out of few docker containers.

- **Hadoop** as batch-layer with Namenode (http://localhost:9870/), Masternode, Datanodes 
and Resourcemanager (http://localhost:8088/)
- **Cassandra** for saving different views
- **Spark** with workers and master (http://localhost:8080/)
- **Grafana** for visualization (http://localhost:3000/)

Every container is running within same network (**lambda-network**).

To validate the data and quality of the data and the results kafka is also writing 
all data to cassandra. While hadoop is processing, spark is catching up
new incoming data and serving them as a real-time-view. Is hadoop done
processing spark resets and catching up from the beginning.

## Grafana Setup

To visualize the whole process you can actually use the kappa and lambda templates 
for grafana. You cana find them in (/grafana_templates/)

### Connect to Apache Cassandra

1. Go to http://localhost:3000
2. login (username=admin, password=admin) # can be changed
3. Go to Connections -> Add new connection -> Apache Cassandra -> Install
4. Go to Connections -> Data sources -> Apache Cassandra
5. Fill up following entry -- Host: cassandra1:9042
6. Save & Test

### Lambda Dashboard

![lambda-architecture](./docs/img/lambda_dashboard.png)
Figure 3: Lambda Dashboard

Lambda dashboard is showing Real Time, Batch and Validation View. Real Time View + Batch View should be
Validation View.


## Analysis and Comparison

### Lambda

In Figure 5 and Figure 6 the blue curve represents the real purchases. In 
Figure 5 there is a small delay for the Batch-View (HadoopResults) to catch up
the real purchases. Meanwhile Real-View (SparkResults) is catching up missing
values.

![lambda-results](./docs/lambda_results_batch_real.png)
Figure 5: Lambda Results

Figure 6 represents Batch-View + Real-View. Where both results are summed up.
Around 01:13:45 there is a small peak, which is above the real purchases. 
Spark couldn't restart the calculation there. Overall the curve is at some
points above the real purchases as well. Reason for this is synchronization.
Spark needs some time to restart and to get the message to restart. Within
this window spark is still catching messages. That's why the curve is at some
points above the real purchases.

![lambda-results](./docs/lambda_results.png)
Figure 6: Lambda Results
