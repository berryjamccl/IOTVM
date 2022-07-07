#！/usr/bin/env python
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


mqtt_server_broker = "public.mqtthq.com"
port_broker = 1883
client_broker = 0

username_tum = "JWT"
jwt_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTM1NzI0NjksImlzcyI6ImlvdHBsYXRmb3JtIiwic3ViIjoiOV8xOCJ9.ozlu4DX80cCf7_t4u_yJIokDWJpKm5rv-YT3xHvjx5xtYDkCWKzy6qq4Ra4UDu-FcyDDf8M4Tela9uLDHQYxbhY0ow7tD2GPtX2uzB8wouwl1QUMJibA0bMFo1zZK_Zo8mVqRV2ZMlE3iwwbcIudxbfboPw__XK73ubfApfx0DHcxVbnWvKZYAYsWReHS89Lto0KLYlZd_1IIzfOXDaVo1QxU9wJ1PLTOsJVpfdsyzO_VKVw5gfFkmI87meBbKT0nCzlKLs1u-1rVu4wov98_gGrVC3koGa2ly8l4dry5KQQrcWulnFZDq-yEsmim6EdNa3UlhDcnpWDj2uv6BHYIPrYvIbiPgpbtqZoYD4B-hie_tTeVYjDDga4WNGHBsGz7B8Vavtr0zzHZ5Ow23VEWvXYMdLdRw6PtdB0ccVYOxTs61DoachViQh-SXJvWa5YBlRlo_jiC8kTwczA9DknYr1kgCG3uD8V2v3zPYE9cFEmeiPPCBSVzRETnqqHronmmK-qpreGfvAquYop6Tfg24ncgEfTsJmd0_rx4EVZT8tNRk17hgR8mLi7uLEa6jcqrgz9yHXU1XiBTdV6JBzD2GSOJcRhhoGwq0IWkx0wBN2TRpfxQeLJQYMukFLCqFS6EevNYK9nwy8-8hbQwd37O46Y9MtVGIMa6EKBIaGQzks"
mqtt_server_tum = "138.246.236.181"
port_tum = 1883
groupname = "group7"
userid = 9
deviceid = 18
sensor_name = "forecast receiver"
topic = "9_18"
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
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    requests.packages.urllib3.disable_warnings()
    session.get('https://138.246.236.111:31001/api/v1/web/guest/default/func-9-action-modeljaegermodified-109', verify=False)
    response = minioClient.fget_object(bucket_name="iot-group-7", object_name="jaeger_model.pkl",file_path = "jaeger_model.pkl")
    openmodel = open('jaeger_model.pkl', 'rb')
    mod = pickle.load(openmodel)
    openmodel.close()
    return mod



def connect_broker():
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

    client_broker = paho.Client()
    client_broker.on_connect = on_connect

    print("broker connecting")
    client_broker.connect(mqtt_server_broker, port_broker)
    print("broker connected")
    client_broker.on_subscribe = on_subscribe
    client_broker.on_message = on_message
    client_broker.on_publish = on_publish
    client_broker.loop_start()
    return client_broker


def publish_broker(client_broker, msg):
    # msg = "{\"username\":\"%s\",\"%s\":%d,\"device_id\":\"%d\",\"timestamp\":%lu000}" % (
    # groupname, sensor_name, predict_value, deviceid, timestamp)
    print(msg)
    result = client_broker.publish(topic, msg, qos=1)
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to brokerMQ topic `{topic}`")
    else:
        print(f"Failed to send message to brokerMQ topic {topic}")


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
    client_broker = connect_broker()
    client_tum = connect_tum()

    mod = download_model()
    msg = forecast15(mod)
    publish_broker(client_broker, msg)
    publish_tum(client_tum, msg)
    time.sleep(120)
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
            publish_broker(client_broker, msg)
            publish_tum(client_tum, msg)
            time.sleep(120)
