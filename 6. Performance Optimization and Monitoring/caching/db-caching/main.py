import sqlite3
import redis
import json
import hashlib
from fastapi import FastAPI
from pydantic import BaseModel


# establish redis tool connection for caching
app = FastAPI()

#initialize redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)


# establish database connection
def get_db_connection():
    conn = sqlite3.connect('db.sqlite3')
    conn.row_factory = sqlite3.Row
    return conn


# set up database
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor() # get a cursor object
     # create a table
    cursor.execute("""
CREATE TABLE IF NOT EXISTS users(
                   id INTEGER PRIMARY KEY,
                   name TEXT NOT NULL,
                   age INTEGER
                   )
""")
    cursor.execute("INSERT INTO users (id, name, age) VALUES (1, 'Michael', 45)")
    cursor.execute("INSERT INTO users (id, name, age) VALUES (2, 'Jim', 35)")
    cursor.execute("INSERT INTO users (id, name, age) VALUES (3, 'Pam', 27)")
    conn.commit()
    conn.close()
# initialize database
init_db()

# define pydantic request model for user query by id 
class UserQuery(BaseModel):
    user_id: int

# function to create a cache key
def make_cache_key(user_id: int):
    raw = f"user:{user_id}"
    return hashlib.sha256(raw.encode()).hexdigest()

# define endpoint to get user by id with caching
@app.post('/get-user')
def get_user(query: UserQuery):
    cache_key = make_cache_key(query.user_id) # create a cache key
    
    cached_data = redis_client.get(cache_key) # check if data is in cache
     # if data is found in cache, return it
    if cached_data:
        print('Serving from Redis Cache!')
        return json.loads(cached_data)
    
    # if data is not found in cache, fetch from database
    conn = get_db_connection()
    cursor = conn.cursor() # get a cursor object
    cursor.execute("SELECT * FROM users WHERE id = ?", (query.user_id,))# execute query
    row = cursor.fetchone()# fetch one record
     # close the database connection
    conn.close()

    if row is None:
        return {'message': 'User not found.'}
    # store the fetched data in cache for future requests
    result = {'id': row['id'], 'name': row['name'], 'age': row['age']}
    # cache the result with an expiration time of 1 hour (3600 seconds)#
    redis_client.setex(cache_key, 3600, json.dumps(result))
    print('Fetched from DB and Cached!')

    return result