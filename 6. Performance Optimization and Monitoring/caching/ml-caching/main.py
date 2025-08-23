import redis
import json
import hashlib
import joblib
from fastapi import FastAPI
from pydantic import BaseModel

# establish redis tool connection for caching
# install redis server and run it locally


#initialize FastAPI app and Redis client
app = FastAPI()
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# load the pre-trained model
model = joblib.load('model.joblib')

# define the data model for the input
# this should match the input features of the model 
class IrisFlower(BaseModel):
    SepalLengthCm: float
    SepalWidthCm: float
    PetalLengthCm: float
    PetalWidthCm: float

 # define the method to convert the input data to a list(bcos user input is in json format(dict))
    def to_list(self):
        return [
            self.SepalLengthCm,
            self.SepalWidthCm,
            self.PetalLengthCm,
            self.PetalWidthCm
        ]
    # define the method to create a unique cache 
    # this will be used to store and retrieve predictions from the cache
    # we use a hash of the JSON representation of the input data to ensure uniqueness
    # sort_keys=True ensures that the order of keys does not affect the hashSSS
    
    def cache_key(self):
        raw = json.dumps(self.model_dump(), sort_keys=True)
        return f"Predict: {hashlib.sha256(raw.encode()).hexdigest()}"
    

# define the endpoint for prediction
# it will first check if the prediction is in the cache
# if it is, it will return the cached result
# if not, it will make a prediction using the model and store the result in the cache
# the cache will expire after 1 hour (3600 seconds)

@app.post('/predict')
async def predict(data: IrisFlower):
    key = data.cache_key()

    cached_result = redis_client.get(key)
    if cached_result:
        print('Serving prediction from Cache!')
        return json.loads(cached_result)
    
    # make a prediction using the model   
    prediction = model.predict([data.to_list()])[0]
    result = {'prediction': int(prediction)}
    # store the result in the cache (as a JSON string in Redis)
    redis_client.set(key, json.dumps(result), ex=3600)
    return result