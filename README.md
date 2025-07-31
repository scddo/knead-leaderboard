# Stock Trading Leaderboard (MySQL + Python)

This project simulates a stock trading leaderboard system using Python and MySQL. It creates a database, sets up required tables from a schema file, and populates them with randomly generated user and trade data using the `Faker` library.

## Features

- Creates a MySQL database and tables via a schema file
- Generates fake users and stock trades using Python Faker library
- Batches inserts to avoid performance issues

## Prerequisites

- Python 3.7+
- MySQL Server

**Ensure MySQL is running and a user like root has the proper credentials.**


Edit `databaseHandler.py` and set your MySQL credentials at the top of the file:
```python
mySqlUserName = "root"
mySqlPassword = "your_password"
mySqlHost = "localhost"
myDatabase = "leaderboard"
```
## Usage
1. **Ensure MySQL is running and a user like root has the proper credentials.**
Edit `databaseHandler.py` and set your MySQL credentials at the top of the file:
```python
mySqlUserName = "root"
mySqlPassword = "your_password"
mySqlHost = "localhost"
myDatabase = "leaderboard"
```

2. **Set up and activate the virtual environment:**
```bash
python -m venv venv
venv/Scripts/activate #or source venv/bin/activate on mac and linux
```

3. **Install the dependencies:**
```bash
pip install -r requirements
```

4. **Simply run the script:**
```bash
python databaseHandler.py
```
This will generate 100 users and 10,000 fake trades by default. However this can be editted using the parameters in the `populate_db()` function.

License

This project is open source and available under the MIT License.
