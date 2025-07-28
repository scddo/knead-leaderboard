import random
from faker import Faker
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import errorcode

fake = Faker()
tickers = ['AAPL', 'GOOG', 'TSLA', 'MSFT', 'AMZN', 'NFLX', 'NVDA', 'META', 'BABA', 'AMD']

mySqlUserName = "root"
mySqlPassword = "!Soccer19"
mySqlHost = "localhost"
myDatabase = "leaderboard"

#configure the MySQL connection
mydb = mysql.connector.connect(  
    host=mySqlHost,
    user=mySqlUserName,
    password=mySqlPassword,
)
mycursor = mydb.cursor()

#function to generate the database and tables if they don't exist
def generate_db():
    try:
        
        #create the database if it does not exist
        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {myDatabase}")
            print(f"Database `{myDatabase}` created or already exists.")
        except mysql.connector.Error as err:
            print(f"Failed creating database: {err}")
            exit(1)
        mycursor.execute(f"USE {myDatabase}")

        #read from the schema file
        with open('schema.txt', 'r') as schema_file:
            schema = schema_file.read()

        #execute the schema by splitting the file
        for statement in schema.strip().split(';'):
            if statement.strip(): 
                mycursor.execute(statement)

        #list tables  
        mycursor.execute("SHOW TABLES")
        for x in mycursor:
          print(x)

    #error handling
    except mysql.connector.Error as err:
        print("MySQL Error:", err)

def randomDate():
    now = datetime.now()
    days_back = random.randint(0, 180)
    random_time = timedelta(days=days_back, hours=random.randint(0, 23), minutes=random.randint(0, 59))
    return (now - random_time).strftime('%Y-%m-%d %H:%M:%S')

def populate_db(userCount, tradeCount):
    print(f"Inserting {userCount} users.")
    for _ in range(userCount):
        username = fake.user_name()
        mycursor.execute("INSERT INTO users (username) VALUES (%s)", (username,))
        mydb.commit()
    print("Complete.")

    #get a list of the user IDs
    mycursor.execute("SELECT id FROM users")
    rows = mycursor.fetchall()
    userIDs = [row[0] for row in rows]


    print(f"Inserting {tradeCount} trades.")
    #batch inserts to improve performance
    trades_batch = []
    BATCH_SIZE = 1000
    for _ in range(tradeCount):
        user_id = random.choice(userIDs)
        ticker = random.choice(tickers)
        quantity = random.randint(1, 1000)
        price = round(random.uniform(10, 1000), 2)
        profit_loss = round(random.uniform(-500, 500), 2)
        executed_at = randomDate()
        trades_batch.append((user_id, ticker, quantity, price, profit_loss, executed_at))
        for i in range(0, len(trades_batch), BATCH_SIZE):
            batch = trades_batch[i:i + BATCH_SIZE]
            mycursor.executemany("INSERT INTO trades (user_id, ticker, quantity, price, profit_loss, executed_at) VALUES (%s, %s, %s, %s, %s, %s)", batch)
            mydb.commit()
            print(f"Inserted {i + len(batch)} trades into the database.")
       
    mycursor.commit()
    trades_batch.clear()

    
    print("Complete.")

if __name__ == "__main__":
    #if it's giving errors, use this drop db command to regenerate and repopulate the db from scratch
    #mycursor.execute("drop database if exists leaderboard")

    #only generate the database if it does not exist already
    mycursor.execute(f"SHOW DATABASES LIKE '{myDatabase}'")
    result = mycursor.fetchone()
    if not result:
        generate_db()
        populate_db(100, 10000)
    mycursor.execute(f"USE {myDatabase}")
    mycursor.execute("SELECT COUNT(*) FROM users")
    print(mycursor.fetchall())
    #close the connection when the code is done
    if mydb.is_connected():
                mycursor.close()
                mydb.close()