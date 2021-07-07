#! python
import sys
import time
import subprocess

import pika


HDFS_OUTPUT="/image-search-indexing/output/{}"
HDFS_COMMAND="/opt/hadoop-3.2.1/bin/hdfs dfs -fs hdfs://p43.arquivo.pt:9000 -ls -C {}"

def main(args):
    SOLR_COLLECTION = args[1]
    p = subprocess.Popen(HDFS_COMMAND.format(HDFS_OUTPUT.format(SOLR_COLLECTION)),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    path = ""
    for line in p.stdout.readlines():
        line = line.decode("utf-8").strip()
        if line > path:
            path = line

    print(HDFS_COMMAND.format(path))
    p = subprocess.Popen(HDFS_COMMAND.format(path),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    connection = pika.BlockingConnection(pika.ConnectionParameters('p90.arquivo.pt'))
    channel = connection.channel()
    channel.queue_declare(queue='log')
    channel.queue_declare(queue='nsfw')

    for line in p.stdout.readlines():
        body = line.decode("utf-8").strip()
        if "part-" in body:
            channel.basic_publish(exchange='', routing_key='log', body="{},{},{}".format("nsfw",time.time(),body))
            channel.basic_publish(exchange='', routing_key='nsfw', body=body)

    connection.close()
        
if __name__ == "__main__":
    main(sys.argv)
