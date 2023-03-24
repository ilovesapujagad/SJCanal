import paramiko
from paramiko import SSHClient
from base64 import decodebytes
from flask import Flask, request, jsonify,make_response
import json
import os
import requests
from sys import stderr
import time
import socket
import re
app = Flask(__name__)

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'


@app.route("/connectorname/<connectorname>")
def connectorbyid(connectorname):
    try:
        url = "http://10.10.65.8:8083/connectors"
        response = requests.get(url)
        x = len(response.json())
        liststatus = []
        listsconfig = []

        
        url = "http://10.10.65.60:8083/connectors/"+ connectorname +"/status"
        response1 = requests.get(url)
        liststatus.append(response1.json())

        url = "http://10.10.65.60:8083/connectors/"+ connectorname +"/config"
        response1 = requests.get(url)
        listsconfig.append(response1.json())
        dicti={}
        dicti["response"] = {"config":listsconfig[0]},{"status":liststatus[0]}

        
        return jsonify(dicti),response1.status_code

    
    except Exception as e:
        print(e)
        return jsonify({'status':'error'}),403

@app.post("/connection/oracle")
def connctionoracle():
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
    config_table_inclusion_regex = request_data['config']["table.inclusion.regex"]
    url = "http://10.10.65.60:8083/connectors"
    jsons = {
        "name": str(name),
        "config": {
            "connector.class": str(config_connect_class), #dinamis
            "name": str(config_name), #dinamis
            "tasks.max": 1,
            "confluent.topic.bootstrap.servers": "http://10.10.65.60:9092",
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
            "redo.log.consumer.bootstrap.servers": "http://10.10.65.60:9092",
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
    return response.json(),response.status_code


@app.post("/connection/mysql")
def connctionmysql():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400
    request_data = request.get_json()
    name = request_data['name']
    config_connect_class = request_data['config']["connector.class"]
    config_connect_url = request_data['config']['connection.url']
    config_connect_username = request_data['config']["connection.user"]
    config_connect_password = request_data['config']["connection.password"]
    config_table_whitelist = request_data['config']["table.whitelist"]
    config_mode = request_data['config']['mode']
    config_timestamp_column_name = request_data['config']['timestamp.column.name']
    config_poll_interval_ms = request_data['config']['poll.interval.ms']
    url = "http://10.10.65.60:8083/connectors"
    jsons = {
            "name": name,  #dinamis
            "config": {
                "connector.class": config_connect_class,  #dinamis
                "key.converter": "io.confluent.connect.avro.AvroConverter",  #statis
                "key.converter.schema.registry.url": "http://10.10.65.60:8081",   #statis
                "value.converter": "io.confluent.connect.avro.AvroConverter",  #statis
                "value.converter.schema.registry.url": "http://10.10.65.60:8081",   #stais
                "tasks.max": 1,   #statis
                "connection.url": config_connect_url,   #dinamis
                "connection.user": config_connect_username,  #dinamis
                "connection.password": config_connect_password,  #dinamis
                "table.whitelist": config_table_whitelist,  #dinamis (table)
                "mode": config_mode,  #dinamis (timestamp, increment) <select option>
                "timestamp.column.name": config_timestamp_column_name,  #dinamis  (from name field)
                "topic.prefix": "mariadb-",  #statis
                "poll.interval.ms": config_poll_interval_ms  #dinamis
            }
        }

    response = requests.post(url,json=jsons)
    return response.json(),response.status_code


@app.post("/connection/sqlserver")
def connctionmysqlserver():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400
    request_data = request.get_json()
    name = request_data['name']
    config_connect_class = request_data['config']["connector.class"]
    config_database_hostname = request_data['config']['database.hostname']
    config_database_port = request_data['config']['database.port']
    config_connect_username = request_data['config']["database.user"]
    config_connect_password = request_data['config']["database.password"]
    config_database_name = request_data['config']['database.names']
    config_database_whitelist = request_data['config']["database.whitelist"]
    config_table_include_list = request_data['config']['table.include.list']
    url = "http://10.10.65.60:8083/connectors"
    jsons = {
            "name": name,  #dinamis
            "config": {
                "connector.class": config_connect_class,  #dinamis
                "tasks.max": "1",  #statis
                "topic.prefix": "sqlserver-",  #static
                "database.hostname": config_database_hostname,  #dinamis
                "database.port": config_database_port,  #dinamis
                "database.user": config_connect_username ,  #dinamis
                "database.password": config_connect_password,  #dinamis
                "database.names": config_database_name,  #dinamis
                "database.server.name": "unique-sj",  #static
                "database.whitelist": config_database_whitelist,  #dinamis (database name)
                "table.include.list":  config_table_include_list,   #dinamis (table name)
                "database.history.kafka.bootstrap.servers": "10.10.65.60:9092",  #statis
                "database.history.kafka.topic": "sqlserver-",  #statis
                "database.encrypt": False,  #statis
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",  #statis
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",   #statis
                "typeClassName": "org.apache.pulsar.common.schema.KeyValue",   #statis
                "database.history": "org.apache.pulsar.io.debezium.PulsarDatabaseHistory",   #statis
                "database.tcpKeepAlive": "true",  #statis
                "decimal.handling.mode": "double"  #statis
            }
        }

    response = requests.post(url,json=jsons)
    return response.json(),response.status_code



@app.get("/connector")
def connector():
    try:
        token = request.headers.get('Authorization')
        url = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/auth/verify-token"
        headers = {"Authorization": token}
        response = requests.post(url,headers=headers)
        getid = response.json()["userId"]
        url = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/KafkaConnect?$select[0]=connector&$select[1]=_id&user="+getid+""
        response = requests.get(url,headers=headers)
        if response.json() :
            total_connect = len(response.json())
            list_connect = response.json()
        else :
            return jsonify({'status':'not have connector'}),200
        liststatus = []
        listsconfig = []
        
        try:
            for i in range(0, total_connect):
                # urlkafkaconnect = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/KafkaConnect/"+ list_connect[i] +"?$lookup=*"
                # response = requests.get(urlkafkaconnect,headers=headers)
                # name_connect = response.json()
                # # print(response.json())
                url = "http://10.10.65.60:8083/connectors/"+list_connect[i]["connector"]+"/status"
                response1 = requests.get(url)
                liststatus.append(response1.json())
                if response1.status_code == 404:
                    url = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/KafkaConnect/"+list_connect[i]["_id"]+""
                    requests.delete(url,headers=headers)
                url = "http://10.10.65.60:8083/connectors/"+list_connect[i]["connector"]+"/config"
                response2= requests.get(url)
                listsconfig.append(response2.json())

        except Exception as e:
            print(e)
            return jsonify({'status':'error database Or Connector, please relogin or check microgen'}),403
        dicti = {}
        for i in range(0, total_connect):
            dicti[i]={"config":listsconfig[i]},{"status":liststatus[i]}
        
        return jsonify(dicti),200
    
    except Exception as e:
        print(e)
        return jsonify({'status':'error'}),403
    
@app.route("/kafka/message/<topic>")
def kafka_messages(topic):

    def ssh_con (ip, un, pw):
        global client
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print ("Connecting to device/VM: %s" % ip)
        client.connect(ip, username=un, password=pw)


    def cmd_io (command):
        client_cmd
        client_cmd.send("%s \n" %command)
        time.sleep(1)
        output = client_cmd.recv(10000).decode("utf-8")
        # print (output)

    # ip = raw_input("Enter WAG IP : ")
    # ip  = sys.argv[1]

    ip = '10.10.65.60'
    un = 'ubuntu'
    pw = '2wsx1qaz'

    ssh_con(ip,un,pw)
    client_cmd = client.invoke_shell()

    # print ("SSH CONNECTION ESTABLISHED TO vMEG %s" % ip)
    cmd_io ("sudo docker exec -it ubuntu-kafka /opt/confluent/bin/kafka-avro-console-consumer --topic "+topic+" --bootstrap-server localhost:9092 --from-beginning")
    # cmd_io ("ls /usr/bin")
    # cmd_io ("debug wag https")
    # cmd_io ("debug wag httpc")
    # cmd_io ("debug https")
    # cmd_io ("debug httpc")
    # cmd_io ("debug wag kafka")
    
    check = True
    list_message = []
    dict_message={}
    client_cmd.settimeout(1.0)
    time.sleep(2)
    while check:
        try:
            output = client_cmd.recv(10000).decode("utf-8")
            list_message.append({"message": output})
            print (output)
        except socket.timeout:
            dict_message["message"]=list_message
            check = False
            pass
    return dict_message,200

@app.route("/kafka/consumer/groups/<topic>")
def consumer_groups(topic):
    try:
        def ssh_con (ip, un, pw):
            global client
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print ("Connecting to device/VM: %s" % ip)
            client.connect(ip, username=un, password=pw)


        def cmd_io (command):
            client_cmd 
            client_cmd.send("%s \n" %command)
            time.sleep(1)
            output = client_cmd.recv(10000).decode("utf-8")
            # print (output)

        # ip = raw_input("Enter WAG IP : ")
        # ip  = sys.argv[1]

        ip = '10.10.65.60'
        un = 'ubuntu'
        pw = '2wsx1qaz'

        ssh_con(ip,un,pw)
        client_cmd = client.invoke_shell()

        cmd_io (f"sudo docker exec -it ubuntu-kafka /opt/confluent/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe | grep -E '{topic}'")

        check = True
        list_output = []
        y={}
        client_cmd.settimeout(1.0)
        while check:
            try:
                output = client_cmd.recv(10000).decode("utf-8")
                ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
                output = ansi_escape.sub('', output)
                output= output.replace('\r\n', ' ')
                output = output.split()
                list_output += output
                print (output)
            except socket.timeout:
                list_output.pop()
                y["message"]=list_output
                check = False
                pass
        size = 9
        sub_lists = [list_output[i:i+size] for i in range(0, len(list_output), size)]
        n=len(sub_lists)
        print(sub_lists[1])
        # return y
        list_consumer = []
        for i in range(0, n):
            if i == i:
                print(i)
                list_consumer.append({'GROUP':sub_lists[i][0], 'TOPIC':sub_lists[i][1], 'PARTITION':sub_lists[i][2], 'CURRENT-OFFSET':sub_lists[i][3], 'LOG-END-OFFSET':sub_lists[i][4], 'LAG':sub_lists[i][5], 'CONSUMER-ID':sub_lists[i][6], 'HOST':sub_lists[i][7], 'CLIENT-ID':sub_lists[i][8]}) 
            else:
                pass
        return list_consumer,200
    except Exception as e:
        return jsonify({'status':str(e)}),403
    





if __name__ == "__main__":
    app.run(debug=True)
