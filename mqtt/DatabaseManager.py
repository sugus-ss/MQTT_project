import json
import sqlite3

# SQLite DB Name
DB_Name =  "Home.db"

# Database Manager Class
class DatabaseManager():
	def __init__(self):
		self.conn = sqlite3.connect(DB_Name)
		self.conn.execute('pragma foreign_keys = on')
		self.conn.commit()
		self.cur = self.conn.cursor()
		
	def add_del_update_db_record(self, sql_query, args=()):
		self.cur.execute(sql_query, args)
		self.conn.commit()
		return

	def __del__(self):
		self.cur.close()
		self.conn.close()

# Function to save Temperature to DB Table
	def Data_Handler(self,jsonData):
	#Parse Data 
		Time = jsonData['Time']
		Temperature = jsonData['Temperature']
		Humidity = jsonData['Humidity']
		ThermalArray = jsonData['ThermalArray']
	
	#Push into DB Table
		
		self.add_del_update_db_record("insert into Home_Data (Time, Temperature, Humidity, ThermalArray) values (?,?,?,?)",[Time, Temperature, Humidity, ThermalArray])
		
		print ("Inserted Temperature Data into Database.")
		print ("")

# Master Function to Select DB Funtion based on MQTT Topic

	def sensor_Data_Handler(self,Topic, jsonData):
		if Topic == "Home":
			self.Data_Handler(jsonData)