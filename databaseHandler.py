import random
from faker import Faker
from datetime import datetime, timedelta
from datetime import timezone
import mysql.connector
from mysql.connector import errorcode, pooling
from flask import Flask, jsonify, render_template

fake = Faker()
tickers = ['AAPL', 'GOOG', 'TSLA', 'MSFT', 'AMZN', 'NFLX', 'NVDA', 'META', 'BABA', 'AMD']

mySqlUserName = " "
mySqlPassword = " "
mySqlHost = " "
myDatabase = " "

app = Flask(__name__)
@app.route('/')
def home():
    return render_template('index.html')

#configure the MySQL connection
mydb = mysql.connector.connect(  
    host=mySqlHost,
    user=mySqlUserName,
    password=mySqlPassword,
)
myCursor = mydb.cursor(buffered=True, dictionary=True)
dbconfig = {
    "host": mySqlHost,
    "user": mySqlUserName,
    "password": mySqlPassword,
}
cnxpool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    pool_reset_session=True,
    **dbconfig
)

def get_connection():
    conn = cnxpool.get_connection()
    conn.database = myDatabase
    return conn

#function to generate the database and tables if they don't exist
def generate_db():
    try:
        myCursor = mydb.cursor(buffered=True, dictionary=True)
        #create the database if it does not exist
        try:
            myCursor.execute(f"CREATE DATABASE IF NOT EXISTS {myDatabase}")
            print(f"Database `{myDatabase}` created or already exists.")
        except mysql.connector.Error as err:
            print(f"Failed creating database: {err}")
            exit(1)
        myCursor.execute(f"USE {myDatabase}")

        #read from the schema file
        with open('schema.txt', 'r') as schema_file:
            schema = schema_file.read()

        #execute the schema by splitting the file
        for statement in schema.strip().split(';'):
            if statement.strip(): 
                myCursor.execute(statement)

        #list tables  
        myCursor.execute("SHOW TABLES")
        for x in myCursor:
          print(x)

    #error handling
    except mysql.connector.Error as err:
        print("MySQL Error:", err)
    

def randomDate(force_today=False):
    now = datetime.now()
    #forced fallback logic because the random trades never wanted to be on the current day
    if force_today:
        days_back = 0
    else:
        days_back = random.choices([0, random.randint(1, 180)], weights=[0.2, 0.8])[0]
    days_back = random.randint(0, 180)
    hour = random.randint(9,16)  #regular trading hours
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    random_time = timedelta(days=days_back, hours=hour, minutes=minute, seconds=second)
    dt = now - random_time
    if dt.strftime("%H") < "09" or dt.strftime("%H") > "16": #if the time is outside of trading hours, set it to the next trading day at 9:00 AM
        dt = dt.replace(hour=9, minute=0, second=0) + timedelta(days=1)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def unique_username():
    username = fake.user_name()
    myCursor.execute("SELECT COUNT(*) as count FROM users WHERE username = %s", (username,))
    count = myCursor.fetchone()['count']
    if count > 0:
        return unique_username()
    return username

def populate_db(userCount, tradeCount):
    myCursor = mydb.cursor(buffered=True, dictionary=True)
    if not mydb.is_connected():
        mydb.reconnect()
    print(f"Inserting {userCount} users.")
    for _ in range(userCount):
        username = unique_username()
        myCursor.execute("INSERT INTO users (username) VALUES (%s)", (username,))
        mydb.commit()
    print("Complete.")

    #get a list of the user IDs
    myCursor.execute("SELECT id FROM users")
    rows = myCursor.fetchall()
    userIDs = [row['id'] for row in rows]

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
        executed_at = randomDate(force_today=(_ < 50)) #force the first 50 trades to be today
        trades_batch.append((user_id, ticker, quantity, price, profit_loss, executed_at))
    myCursor.executemany("INSERT INTO trades (user_id, ticker, quantity, price, profit_loss, executed_at) VALUES (%s, %s, %s, %s, %s, %s)", trades_batch)
    mydb.commit()
    trades_batch.clear()

    print("Complete.")


def generate_leaderboard(period):
    conn = get_connection()
    myCursor = conn.cursor(buffered=True, dictionary=True)
    if not mydb.is_connected():
        mydb.connect()
    myCursor.execute(f"USE {myDatabase}")

    if period == 1: #daily
        query = f"""
            SELECT u.username, 
                ROUND(SUM(t.profit_loss), 2) AS total_pnl
            FROM trades t
            JOIN users u ON t.user_id = u.id
            WHERE DATE(t.executed_at) = CURDATE()
            GROUP BY u.id
            ORDER BY total_pnl DESC
            LIMIT 10;
        """
    elif period == 2: #monthly
        query = f"""
            SELECT u.username, 
                ROUND(SUM(t.profit_loss), 2) AS total_pnl
            FROM trades t
            JOIN users u ON t.user_id = u.id
            WHERE YEAR(t.executed_at) = YEAR(CURDATE())
              AND MONTH(t.executed_at) = MONTH(CURDATE())
            GROUP BY u.id
            ORDER BY total_pnl DESC
            LIMIT 10;
        """
    elif period == 3: #all-time
        query = f"""
            SELECT u.username, 
                ROUND(SUM(t.profit_loss), 2) AS total_pnl
            FROM trades t
            JOIN users u ON t.user_id = u.id
            GROUP BY u.id
            ORDER BY total_pnl DESC
            LIMIT 10;
        """




    myCursor.execute(query)

    #add a column to rank the sorted data
    results = myCursor.fetchall()
    ranked_results = []
    for i, row in enumerate(results, 1):
        ranked_results.append({
            'rank': i,
            'username': row['username'],
            'total_pnl': float(row['total_pnl'])
        })
    myCursor.close()
    conn.close()
    return ranked_results

@app.route('/api/leaderboard/all-time')
def api_all_time():
    data = generate_leaderboard(3)
    return jsonify(data)

@app.route('/api/leaderboard/daily')
def api_daily():
    data = generate_leaderboard(1)
    return jsonify(data)

@app.route('/api/leaderboard/monthly')
def api_monthly():
    data = generate_leaderboard(2)
    return jsonify(data)
    

if __name__ == "__main__":
    myCursor = mydb.cursor(buffered=True, dictionary=True)
    #if it's giving errors, use this drop db command to regenerate and repopulate the db from scratch
    myCursor.execute("drop database if exists leaderboard")

    #only generate the database if it does not exist already
    myCursor.execute(f"SHOW DATABASES LIKE '{myDatabase}'")
    result = myCursor.fetchone()
    if not result:
        generate_db()
        populate_db(100, 10000)

    myCursor.execute(f"SELECT DATE(executed_at), COUNT(*) FROM trades GROUP BY DATE(executed_at) ORDER BY DATE(executed_at) DESC LIMIT 5;")
    result = myCursor.fetchone()
    print(result)
    app.run(debug=True)
