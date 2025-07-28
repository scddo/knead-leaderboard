import mysql.connector

mySqlUserName = "root"
mySqlPassword = "!Soccer19"
mySqlHost = "localhost"

mydb = mysql.connector.connect(
  host=mySqlHost,
  user=mySqlUserName,
  password=mySqlPassword
)

mycursor = mydb.cursor()

#create a database
mycursor.execute("CREATE DATABASE leaderboard")

#read from the schema file
f = open('schema.txt', 'r')
schema = f.read()

#execute the schema
mycursor.execute(schema)

#drop the database before the script ends
mycursor.execute("DROP DATABASE leaderboard")