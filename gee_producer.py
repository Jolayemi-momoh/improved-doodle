import ee
import datetime
import json
import time
from kafka import KafkaProducer

def initialize_gee():
    """Initializes the Google Earth Engine API."""
    try:
        # Try to initialize normally first
        ee.Initialize(project='jolayemi-momoh')
        print("Earth Engine successfully initialized.")
    except Exception as e:
        print("\nLocal credentials not found. Triggering Python authentication...")
        # Force the Python library to request and save the token itself
        ee.Authenticate(auth_mode='notebook')
        ee.Initialize(project='jolayemi-momoh')
        print("Earth Engine successfully initialized.")
def get_agricultural_payload(coordinates, start_date, end_date):
    """
    Fetches agricultural data from Sentinel-2 for specified region and timeframe
    coordinates format: [longitude, latitude]
    """
    #define buffer region around target coordinatesin this case 2km
    point = ee.Geometry.Point(coordinates)
    region = point.buffer(2000)
    #filter Sentinel-2 Surface Reflectance collection
    image_collection=(
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(region)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE',10))
        .sort('CLOUDY_PIXEL_PERCENTAGE')
    )
    #grab clearest image from the timeframe
    target_image =image_collection.first()
    
    if target_image is None:
        print("No low-cloud images found for the specified timeframe.")
        return None
    #select relevant bands for agriculture. in this case red(B4), near-infared(B8) required for crop stress
    nvdi= ndvi = target_image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    mean_ndvi= ndvi.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=10 #Sentinel-2 resolution is 10m/pixel for these bands
    ).getInfo()
    
    #structure how kafka will receive Payload
    payload = {
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "location": {
            "longitude": coordinates[0],
            "latitude": coordinates[1]
        },
        "metrics": {
            "mean_ndvi": mean_ndvi.get('NDVI')
        },
        "source": "Sentinel-2_Harmonized"
    }
    return payload

def setup_kafka_producer():
    """Initializes the kafka producer to connect to our local docker broker"""
    return KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

if __name__ == "__main__":
    initialize_gee()
    
    producer = setup_kafka_producer()
    topic_name = 'crop-telemetry'
    
    #example Target: An agricultural valley coordinate(-26.5,31.3)
    # Array of coordinates representing different farming zones
    farm_sectors = [
        [31.3, -26.5], # Sector Alpha
        [31.4, -26.6], # Sector Beta
        [31.2, -26.4]  # Sector Gamma
    ]
    start= '2026-01-01'
    end= '2026-05-01'
    
    print(f"Starting pipeline stream to Kafka topic: '{topic_name}'...\n")
    
    for i, coords in enumerate(farm_sectors):
        print(f"Scanning Sector {i+1} at coordinates {coords}...")
        data_payload = get_agricultural_payload(coords, start, end)
        
        if data_payload:
            # Push the payload to Kafka
            producer.send(topic_name, data_payload)
            print(f"--> Successfully streamed to Kafka: NDVI = {data_payload['metrics']['mean_ndvi']:.4f}")
        else:
            print("--> No clear imagery found. Skipping.")
            
        # 3-second delay to simulate real-time processing intervals
        time.sleep(3)
        
    # Flush ensures all messages are sent before the script closes
    producer.flush()
    print("\nBatch streaming complete.")