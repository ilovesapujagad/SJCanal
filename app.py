import paramiko
from paramiko import SSHClient
from base64 import decodebytes
from flask import Flask, request, jsonify,make_response
import json
import os
import requests
from sys import stderr
app = Flask(__name__)

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

@app.route("/connectorname/<connectorname>")
def connectorbyid(connectorname):
    try:
        url = "http://10.10.65.5:8083/connectors"
        response = requests.get(url)
        x = len(response.json())
        liststatus = []
        listsconfig = []

        
        url = "http://10.10.65.5:8083/connectors/"+ connectorname +"/status"
        response1 = requests.get(url)
        liststatus.append(response1.json())

        url = "http://10.10.65.5:8083/connectors/"+ connectorname +"/config"
        response1 = requests.get(url)
        listsconfig.append(response1.json())
        dicti={}
        dicti["response"] = {"config":listsconfig[0]},{"status":liststatus[0]}

        
        return jsonify(dicti)

    
    except Exception as e:
        print(e)
        return jsonify({'status':'error'}),403

@app.post("/connection/oracle")
def registeruser():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400
    request_data = request.get_json()
    name = request_data['name']
    config_connect_class = request_data['config']["connector.class"]
    config_name = request_data['config']["name"]
    config_oracle_server = request_data['config']["oracle.server"]
    config_oracle_port = request_data['config']["oracle.port"]
    config_oracle_sid = request_data['config']["oracle.sid"]
    config_oracle_username = request_data['config']["oracle.username"]
    config_oracle_password = request_data['config']["oracle.password"]
    config_table_inclusion_regex = str(request_data['config']["table.inclusion.regex"])
    url = "http://10.10.65.5:8083/connectors"
    jsons = {
        "name": str(name),
        "config": {
            "connector.class": str(config_connect_class), #dinamis
            "name": str(config_name), #dinamis
            "tasks.max": 1,
            "confluent.topic.bootstrap.servers": "http://10.10.65.5:9092",
            "oracle.server": str(config_oracle_server), #dinamis
            "oracle.port": config_oracle_port, #dinamis
            "oracle.sid": config_oracle_sid, #dinamis
            "oracle.username": config_oracle_username, #dinamis
            "oracle.password": config_oracle_password, #dinamis
            "start.from": "snapshot",
            "table.inclusion.regex": config_table_inclusion_regex , #dinamis
            "table.exclusion.regex": "", 
            "table.topic.name.template": "${fullyQualifiedTableName}",
            "topic.prefix": "oracle-",
            "poll.interval.ms": 1000,
            "connection.pool.max.size": 20,
            "confluent.topic.replication.factor": 1,
            "redo.log.consumer.bootstrap.servers": "http://10.10.65.5:9092",
            "topic.creation.groups": "redo",
            "topic.creation.redo.include": "redo-log-topic",
            "topic.creation.redo.replication.factor": 1,
            "topic.creation.redo.partitions": 1,
            "topic.creation.redo.cleanup.policy": "delete",
            "topic.creation.redo.retention.ms": 1209600000,
            "topic.creation.default.replication.factor": 1,
            "topic.creation.default.partitions": 1,
            "topic.creation.default.cleanup.policy": "delete"
        }
    }
    response = requests.post(url,json=jsons)
    return response.json()

@app.get("/connector")
def connector():
    try:
        url = "http://10.10.65.5:8083/connectors"
        response = requests.get(url)
        x = len(response.json())
        liststatus = []
        listsconfig = []

        for i in range(0, x):
            url = "http://10.10.65.5:8083/connectors/"+ response.json()[i] +"/status"
            response1 = requests.get(url)
            liststatus.append(response1.json())

        for i in range(0, x):
            url = "http://10.10.65.5:8083/connectors/"+ response.json()[i] +"/config"
            response1 = requests.get(url)
            listsconfig.append(response1.json())

        dicti = {}
        for i in range(0, x):
            dicti[i]={"config":listsconfig[i]},{"status":liststatus[i]}
        
        return jsonify(dicti),200
    
    except Exception as e:
        print(e)
        return jsonify({'status':'error'}),403
    





if __name__ == "__main__":
    app.run(debug=True)
