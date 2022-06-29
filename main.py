import os
import time
import subprocess
import requests
import json
from datetime import datetime
# from elasticsearch6 import Elasticsearch
from elasticsearch import Elasticsearch
import pandas as pd
from scipy.stats import norm
import statsmodels.api as sm
# from statsmodels.iolib.smpickle import save_pickle, load_pickle
import pickle
import logging
from minio import Minio

import paho.mqtt.client as paho
from paho import mqtt
import io




username_hive = "predict"
pw_hive = "predict7"
mqtt_server_hive = "b380b9e0975944e0aad0cb5f55c35094.s1.eu.hivemq.cloud"
port_hive = 8883
client_hive = 0

username_tum = "JWT"
jwt_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTYzMzQzODUsImlzcyI6ImlvdHBsYXRmb3JtIiwic3ViIjoiOV8zMCJ9.SNjkJ2F-gh-MW13WnrPBXViXx-EtGfmtbVI-p58IrnZ7upSXrPPmdZajJdUsOvcCPZWHnLRfwyz7vEd6NToZkJcsa4AcfYGNtA7zwGIqFrCicc2JCuTJx90uTe2FFTryuzQWmhRDlHrFjQ63vUKQ64uVxINUT4mLvF6T5IK7gmDWvBgKPupZx6JI0Dp72lQLFjOpPLI8ndw16AJRDxGXbMpo1e9uZjAX3WCjStdbYXvpKZP1kbuGJUOoppXDMCAeQ8-ehMP4l2fr3j_4fgfD9feLE30NrfMkncS_7RKZ99Zz-P7m4Ise9Eig1bRqIny1SThPlDZsyPoVBD51RWJIgNce3Zm3Kzqq_uBmsME6Y8AWNwupzBVIZJJwVSXuH2OjSYVJDmOoXNYCVazVUh8ixt_9ewTlq0zJQBxIpXa8vftuCZkwhypupsNix2C4aUBU0FhiDjdr0SRBsndjoFBVcUzG6xZQoq7YSYE8oVjP6hCqIYNuIs9JEDJUKuDo1r8edFLThzDJ6WYYcHnBRAddkGmAFkzy-8eEr_EeBTz1nREk6lRO49MqZKbbm49O9RCv5lhzei18Fnos4a6fXTE_QZX3JpZEpoE2NLYMWEliMs2LiDyIIg5_9arVp1qxDJhnGOqadN65g_fW21x6zvxRmeN8ifw_pQTMvnxoUHdLiUw"
mqtt_server_tum = "138.246.236.181"
port_tum = 1883


# username = "JWT"
groupname = "group7"
userid = 9  #to be found in the platform
deviceid = 18  #to be found in the platform
sensor_name = "forecast receiver"
topic = "9_18" #userID_deviceID
# jwt_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTYzMzQzODUsImlzcyI6ImlvdHBsYXRmb3JtIiwic3ViIjoiOV8zMCJ9.SNjkJ2F-gh-MW13WnrPBXViXx-EtGfmtbVI-p58IrnZ7upSXrPPmdZajJdUsOvcCPZWHnLRfwyz7vEd6NToZkJcsa4AcfYGNtA7zwGIqFrCicc2JCuTJx90uTe2FFTryuzQWmhRDlHrFjQ63vUKQ64uVxINUT4mLvF6T5IK7gmDWvBgKPupZx6JI0Dp72lQLFjOpPLI8ndw16AJRDxGXbMpo1e9uZjAX3WCjStdbYXvpKZP1kbuGJUOoppXDMCAeQ8-ehMP4l2fr3j_4fgfD9feLE30NrfMkncS_7RKZ99Zz-P7m4Ise9Eig1bRqIny1SThPlDZsyPoVBD51RWJIgNce3Zm3Kzqq_uBmsME6Y8AWNwupzBVIZJJwVSXuH2OjSYVJDmOoXNYCVazVUh8ixt_9ewTlq0zJQBxIpXa8vftuCZkwhypupsNix2C4aUBU0FhiDjdr0SRBsndjoFBVcUzG6xZQoq7YSYE8oVjP6hCqIYNuIs9JEDJUKuDo1r8edFLThzDJ6WYYcHnBRAddkGmAFkzy-8eEr_EeBTz1nREk6lRO49MqZKbbm49O9RCv5lhzei18Fnos4a6fXTE_QZX3JpZEpoE2NLYMWEliMs2LiDyIIg5_9arVp1qxDJhnGOqadN65g_fW21x6zvxRmeN8ifw_pQTMvnxoUHdLiUw"
# mqtt_server = "138.246.236.181"
# port = 1883
value = 0.0
connected = False
now = 0
timestamp = 0
timestring = ""
timevalue = 0
predict_value = 0

minioClient = 0
mod = 0

minioAddress = "138.246.236.181:9900"


def connect_minio():
    # Connecting to MinIO
    global minioClient
    # minioClient = Minio("play.min.io")
    minioClient = Minio(minioAddress, access_key='group7', secret_key='A1djIxEDK0', secure=False)
    # print(minioClient.bucket_exists("iot-group-7"))


def download_model():
    # buckets = minioClient.list_buckets()
    # for bucket in buckets:
    #     # print(bucket.name, bucket.creation_date)
    #     logging.info(f"BucketName: {bucket.name}, CreationDate: {bucket.creation_date}")
    requests.get('http://138.246.236.111:31001/api/v1/web/guest/default/func-9-action-modeljaegermodified-109')
    response = minioClient.fget_object(bucket_name="iot-group-7", object_name="jaeger_model.pkl",file_path = "jaeger_model.pkl")
    openmodel = open('jaeger_model.pkl', 'rb')
    mod = pickle.load(openmodel)
    openmodel.close()
    return mod



def connect_hive():
    # setting callbacks for different events to see if it works, print the message etc.
    def on_connect(client, userdata, flags, rc, properties=None):
        print("CONNACK received with code %s." % rc)

    # with this callback you can see if your publish was successful
    def on_publish(client, userdata, mid, properties=None):
        print("mid: " + str(mid))

    # print which topic was subscribed to
    def on_subscribe(client, userdata, mid, granted_qos, properties=None):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    # print message, useful for checking if it was successful
    def on_message(client, userdata, msg):
        print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

    # using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
    # userdata is user defined data of any type, updated by user_data_set()
    # client_id is the given name of the client
    print("hive0")
    client_hive = paho.Client()
    client_hive.on_connect = on_connect
    # set username and password
    client_hive.username_pw_set(username_hive, pw_hive)
    # connect to HiveMQ Cloud on port 8883 (default for MQTT)
    print("hive connecting")
    client_hive.connect(mqtt_server_hive, port_hive)
    print("hive connected")
    # setting callbacks, use separate functions like above for better visibility
    client_hive.on_subscribe = on_subscribe
    client_hive.on_message = on_message
    client_hive.on_publish = on_publish
    # subscribe to all topics of encyclopedia by using the wildcard "#"
    # client_hive.subscribe("encyclopedia/#", qos=1)
    client_hive.loop_start()
    print("hive1")
    return client_hive



def publish_hive(client_hive, msg):
    # msg = "{\"username\":\"%s\",\"%s\":%d,\"device_id\":\"%d\",\"timestamp\":%lu000}" % (
    # groupname, sensor_name, predict_value, deviceid, timestamp)
    print(msg)
    result = client_hive.publish(topic, msg, qos=1)
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to HiveMQ topic `{topic}`")
    else:
        print(f"Failed to send message to HiveMQ topic {topic}")


def connect_tum():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected!\n")
        else:
            print("Failed to connect, return code %d\n", rc)
    client = paho.Client()
    client.username_pw_set(username_tum, jwt_token)
    connect_count = 0
    client.on_connect = on_connect
    client.connect(mqtt_server_tum, port_tum)
    client.loop_start()
    return client


def publish_tum(client, msg):
    msg_count = 0
    result = client.publish(topic, msg)
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to TUM topic `{topic}`")
    else:
        print(f"Failed to send message to TUM topic {topic}")


# def connect_mqtt():
#     global connected
#     def on_connect(client, userdata, flags, rc):
#         global connected
#         if rc == 0:
#             connected = True
#             print("Connected!\n")
#         else:
#             print("Failed to connect, return code %d\n", rc)
#
#     client = mqtt.Client()
#     client.username_pw_set(username, pw)
#     connect_count = 0
#     client.on_connect = on_connect
#     while connect_count < connect_retry:
#         client.connect(mqtt_server, port)
#         if connected:
#             break
#         else:
#             time.sleep(1)
#             connect_count += 1
#     return client
#
#
# def publish(client):
#     msg_count = 0
#     if True:
#         print("try to publish\n")
#         msg = "{\"username\":\"%s\",\"%s\":%d,\"device_id\":\"%d\",\"timestamp\":%lu000}" % (username, sensor_name, predict_value, deviceid, timestamp)
#         publish_count = 0
#         while publish_count < publish_retry:
#             mqtt_result = client.publish(topic, msg)
#             status = mqtt_result[0]
#             if status == 0:
#                 print(f"Send `{msg}` to topic `{topic}`")
#                 msg_count += 1
#                 break
#             else:
#                 print(f"Failed to send message to topic {topic}")
#                 publish_count += 1


def publish2(client):
    msg = "{\"username\":\"%s\",\"%s\":%d,\"device_id\":\"%d\",\"timestamp\":%lu000}" % (groupname, sensor_name, predict_value, deviceid, timestamp)
    print(msg)

def forecast15(mod):
    global now, timestamp, timestring, timevalue, predict_value
    now = int(time.time())
    print(now)
    timestamp = ((now + 15 * 60 + 299) // 300) * 300
    print(timestamp)
    timestring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
    print(timestring)
    timevalue = pd.to_datetime(timestring)
    predict_value = mod.predict(timevalue)
    print(predict_value)
    msg = "{\"username\":\"%s\",\"%s\":%d,\"device_id\":\"%d\",\"timestamp\":%lu000}" % (
    groupname, sensor_name, (predict_value + 0.5), deviceid, timestamp)
    return msg




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connect_minio()
    os.environ['TZ'] = 'Europe/Berlin'
    # time.tzset() #remember to uncomment in linux
    client_hive = connect_hive()
    client_tum = connect_tum()

    # requests.get('http://138.246.236.111:31001/api/v1/web/guest/default/func-9-action-modeljaegermodified-109')
    mod = download_model()
    msg = forecast15(mod)
    publish_hive(client_hive, msg)
    publish_tum(client_tum, msg)
    time.sleep(120)


    # openmodel = open('jaeger_model.pkl', 'rb')
    # mod = pickle.load(openmodel)
    # openmodel.close()

    while (True) :
        # if connected:
        #     time.sleep(60)
        # else:
        #     client = connect_mqtt()
        # time.sleep(60)
        if (int(time.time()) + 900 < timestamp):
            print(int(time.time()))
            print(timestamp)
            print("<, ")
            time.sleep(10)
        else:
            # requests.get('http://138.246.236.111:31001/api/v1/web/guest/default/func-9-action-modeljaegermodified-109')
            mod = download_model()
            msg = forecast15(mod)
            publish_hive(client_hive, msg)
            publish_tum(client_tum, msg)
            time.sleep(120)
