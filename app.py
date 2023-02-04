import paramiko
from paramiko import SSHClient
from base64 import decodebytes
from flask import Flask, request, jsonify,make_response
import json
import os
import requests
from sys import stderr
app = Flask(__name__)

@app.get('/api/kafka/listtopics')
def listtopic(): 
    hostname = '10.10.65.3'
    port = 8080
    username = "sapujagad"
    password = "kayangan"
    listtopics = []


    client = SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password)

    command = "/usr/yava/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper sapujagad-master01.kayangan.com:2181"

    stdin, stdout, stderr = client.exec_command(command)
    for line in stdout.readlines():
        listtopics.append(line.strip())
    dictis = {}
    n = len(listtopics)
    for i in range(n):
        dictis[i]=listtopics[i]
    return jsonify(dictis)
    client.close()
    

def connect_kafka(commands): 
    hostname = '10.10.65.3'
    port = 8080
    username = "sapujagad"
    password = "kayangan"
    responses = {}
    client = SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password)

    command = commands

    stdin, stdout, stderr = client.exec_command(command)
    for line in stdout.readlines():
        responses['result']= str(line.strip())
    return responses
    client.close()

@app.post('/api/kafka/createtopic')
def create_topic():
    request_data = request.get_json()
    replication_factor = int(request_data['replication_factor'])
    partitions = int(request_data['partitions'])
    topic_name = str(request_data['topic_name'])
    response = connect_kafka(f'/usr/yava/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper sapujagad-master01.kayangan.com:2181 --replication-factor {replication_factor} --partitions {partitions} --topic {topic_name}')
    return jsonify(response)

@app.delete('/api/kafka/deletetopic')
def delete_topic():
    try:
        request_data = request.get_json()
        topic_name = str(request_data['topic_name'])
        response = connect_kafka(f'/usr/yava/current/kafka-broker/bin/kafka-topics.sh --zookeeper sapujagad-master01.kayangan.com:2181 --delete --topic {topic_name}')   
        return jsonify({'msg': f'succes delete topic {topic_name}'}),200
    except:
        return jsonify({'msg': 'error server'}), 500

@app.post('/api/kafka/description')
def descriptiontopic():
    try: 
        hostname = '10.10.65.3'
        port = 8080
        username = "sapujagad"
        password = "kayangan"
        my_dict = {}
        request_data = request.get_json()
        topic_name = str(request_data['topic_name'])
        listtopics = []
        client = SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, username=username, password=password)

        command = f'/usr/yava/current/kafka-broker/bin/kafka-topics.sh --zookeeper sapujagad-master01.kayangan.com:2181 --describe --topic {topic_name}'

        stdin, stdout, stderr = client.exec_command(command)
        for line in stdout.readlines():
            listtopics.append(line.strip())
        my_dict={}
        for i in range(len(listtopics)):
            my_dict[i]=listtopics[i]
            
        return jsonify(my_dict),200
        client.close()
    except:
        return jsonify({'msg': 'error server'}), 500




if __name__ == "__main__":
    app.run(debug=True)
