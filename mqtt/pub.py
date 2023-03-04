import paho.mqtt.client as mqtt
import threading
import pandas as pd

# MQTT Settings
MQTT_Broker = "127.0.0.1"
MQTT_Port = 1883
Keep_Alive_Interval = 100
MQTT_Topic = "Home"

def on_connect(client, userdata, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker")
    else:
        print("Connected with MQTT Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    pass

def on_disconnect(client, userdata, rc):
    if rc != 0:
        pass

def on_log(client, userdata, level, buf):
    print("log: ",buf)

mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.on_log = on_log
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
    print("")

#data excel to data frame to json file
excel_file = "SampleInput.xlsx"
df = pd.read_excel(excel_file)

data = df.to_json(orient='index')
print(data)

def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()


# for i, row in data.iterrows():
#     time_str = row["Time"].isoformat()
#     # Create JSON payload
#     # payload = {
#     #     "Time": time_str,
#     #     "Humidity": row["Humidity"],
#     #     "Temperature": row["Temperature"],
#     #     "ThermalArray": row["ThermalArray"],
#     # }
#     payload = {
#     "Time": time_str,
#     "Humidity": row["Humidity"],
#     "Temperature": row["Temperature"],
#     "ThermalArray": row["ThermalArray"],
#     }
#     json_payload = json.dumps(payload, indent=4)

max_size = 250
data_list = []
while data:

    # Slice the string to the maximum byte size:
    segment = data[:max_size]
    # Add the segment to the list:
    data_list.append(segment)
    # Remove the segment from the string:
    data = data[max_size:]
publish_To_Topic(MQTT_Topic+"size",len(data_list))
for segment in data_list:
    publish_To_Topic(MQTT_Topic, segment)

# publish_To_Topic(MQTT_Topic, data)

publish_Fake_Sensor_Values_to_MQTT()

