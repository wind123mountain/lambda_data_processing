import time
import subprocess
import threading


def run_command_in_docker(container_name, command):
    """
    Run a command for a container.

    :param container_name: Name of the Container
    :param command: Command that need to be processed for the container
    """
    try:
        docker_command = ["docker", "exec", container_name] + command
        print(f"Running command in container {container_name}: {' '.join(docker_command)}")
        subprocess.run(docker_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(command)}' failed with error: {e}")


def run_commands_in_parallel(commands):
    """
    Running all commands parallel, to start every process in parallel.

    :param commands: Commands with Container names
    """
    threads = []
    for container_name, command in commands:
        time.sleep(10)
        thread = threading.Thread(target=run_command_in_docker, args=(container_name, command))
        threads.append(thread)
        thread.start()
        print(f"{container_name} started command {command}")

    for thread in threads:
        thread.join()


def main():
    """
    Defining and running the commands.
    """

    commands = [
        ("hadoop", ["/opt/hadoop/bin/hdfs", "dfsadmin", "-safemode", "leave"]),
        ("hadoop", ["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "hdfs://hadoop:8020/checkpoints"]),

        ("spark1", ["/opt/spark/bin/spark-submit",
                          "--master", "spark://spark1:7077",
                        #   "--executor-memory", "2g",
                        #   "--driver-memory", "2g",
                          "--packages", 
                          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
                          "src/speed_layer.py", "broker:29092"]),

        ("spark2", ["/opt/spark/bin/spark-submit",
                          "--master", "spark://spark2:7077",
                          "--executor-cores", "1", 
                          "--driver-cores", "1",
                          "--total-executor-cores", "1",
                          "--packages", 
                          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                          "src/kafka_to_spark_hadoop.py", "broker:29092"]),

        ("broker",
         ["bash", "-c", "python3 src/event_to_kafka.py && tail -f /dev/null"]),


        # ("hadoop", ["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "hdfs://hadoop:8020/data"]),
        # ("hadoop", ["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "hdfs://hadoop:8020/checkpoints"]),
        # ("cassandra1",
        #  ["cqlsh", "-e", "DROP KEYSPACE IF EXISTS event_data_view;"]),
    ]

    run_commands_in_parallel(commands)


if __name__ == "__main__":
    main()
