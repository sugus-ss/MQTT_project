import sqlite3

# SQLite DB Name
DB_Name =  "Home.db"

# SQLite DB Table Schema
TableSchema="""
drop table if exists Home_Data ;
create table Home_Data (
  id integer primary key autoincrement,
  Time text,
  Humidity text,
  Temperature text,
  ThermalArray text
);
"""

#Connect or Create DB File
conn = sqlite3.connect(DB_Name)
curs = conn.cursor()

#Create Tables
sqlite3.complete_statement(TableSchema)
curs.executescript(TableSchema)

#Close DB
curs.close()
conn.close()
