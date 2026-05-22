import json
from kafka import KafkaConsumer
from pymongo import MongoClient

def setup_mongo_connection():
    """ connects to local MongoDB container."""
    #connects to default MongoDB port
    client= MongoClient('mongodb://localhost:27017/')
    
    #Create (or access) the 'agri_pipeline' database annd "spatial_telemetry"
    db=client['agri_pipeline']
    collection= db['spatial_telemetry']
    return collection

def start_consuming():
    """ Listens to kafka and writes messages to MongoDB."""
    # Connect to KRaft Kafka broker
    consumer= KafkaConsumer(
        'crop-telemetry',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest', #Read from the begining if any info was missed
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    ) 
    
    mongo_collection = setup_mongo_connection()
    print("Consumer is active.Listening for UAV/Satelite telemetry...\n")
    
    try:
        for message in consumer:
            payload = message.value
            
            #Insert the JSON payload directly into MongoDB
            insert_result = mongo_collection.insert_one(payload)
            print(f"Stored in MongoDB | ID: {insert_result.inserted_id} | NDVI: {payload['metrics']['mean_ndvi']:.4f}")
    except KeyboardInterrupt:
        print("\nConsumer shut down manually.")
    finally:
        consumer.close()
if __name__ == "__main__":
    start_consuming()