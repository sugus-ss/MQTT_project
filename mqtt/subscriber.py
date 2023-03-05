import paho.mqtt.client as mqtt
import DatabaseManager as dbm
import pandas as pd
import json
import datetime
import time

# MQTT Settings
MQTT_Broker = "127.0.0.1"
MQTT_Port = 1883
Keep_Alive_Interval = 100
MQTT_Topic = "Home"

size = 0
loop = 0
# Subscribe to all Sensors at Base Topic


def on_connect(mosq, obj, flags, rc):
	mqttc.subscribe([(MQTT_Topic, 0),(MQTT_Topic+"size",0)])


combined_payloads = ""
df = ""
# Save Data into DB Table


def on_message(mosq, obj, msg):
	global combined_payloads
	global df
	global size
	global loop
	# This is the Master Call for saving MQTT Data into DB
	# For details of "sensor_Data_Handler" function please refer "sensor_data_to_db.py"
	print("MQTT Data Received...")
	print("MQTT Topic: " + msg.topic)
	if msg.topic == MQTT_Topic+"size":
		size = int(msg.payload.decode())
		print(size)
		loop = 0
	else:	
		print(msg.payload)
		json_payload = msg.payload.decode()
		combined_payloads += json_payload
		print ("--------------------")
		print (json_payload)
		print ("--------------------")
		loop = loop + 1
		print(loop)
		if loop == size:
			data = dict(json.loads(combined_payloads))
			db = dbm.DatabaseManager()
			for key in data.keys():
				row = data[key]
				row['Time'] = datetime.datetime.fromtimestamp(row['Time'] / 1000)
				db.sensor_Data_Handler(MQTT_Topic, row)
			#ยัดเข้า database	

	# a=json.dumps(combined_payloads)
	# json_data = pd.DataFrame.from_dict(loaded, orient="index")
	# print (json_data)

	# Convert the combined payloads to a DataFrame
	# df = pd.read_json(combined_payloads, orient ='index', encoding = 'utf8')
	# print("Data Frame")
	# print(df)

			
	# Iterate over each row in the DataFrame and send the data to the database
	# for i, row in df.iterrows():
	# 	print()
	# 	time_str = row["Time"].isoformat()
	# 	payload = {
	# 			"Time": time_str,
	# 			"Humidity": row["Humidity"],
	# 			"Temperature": row["Temperature"],
	# 			"ThermalArray": row["ThermalArray"],
	# 	}
	# json_s = json.dumps(combined_payloads, indent=4)
	# for key in json_s.keys():

	# 	print(json_s[key])
	# test = pd.DataFrame.to_dict(combined_payloads)
	# print(test)
	# 	print("Row JSON")
	# 	sensor_Data_Handler(MQTT_Topic, json_s)
		

#   เหลือโตงนี้
#	for i, row in# combined_payloads:
#		time_str = row["Time"].isoformat()
#		payload = {
#			"Time": time_str,
#			"Humidity": row["Humidity"],
#			"Temperature": row["Temperature"],
#			"ThermalArray": row["ThermalArray"],
#		}
#		json_s = json.dumps(payload, indent=4)
#		sensor_Data_Handler(msg.topic, json_s)

def on_subscribe(mosq, obj, mid, granted_qos):
    pass

mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
