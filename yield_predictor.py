import pandas as pd
import numpy as np
from pymongo import MongoClient
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error

def fetch_mongo_data():
    """Extracts spatial telemetry data from our MongoDB pipeline."""
    client = MongoClient('mongodb://localhost:27017/')
    db = client['agri_pipeline']
    collection = db['spatial_telemetry']
    
    # Just pull all documents without the confusing projection
    cursor = collection.find()
    
    # Let Python handle the extraction of the nested data
    ndvi_data = []
    for doc in cursor:
        if 'metrics' in doc and 'mean_ndvi' in doc['metrics']:
            ndvi_data.append(doc['metrics']['mean_ndvi'])
            
    return ndvi_data

def generate_training_dataset(ndvi_list):
    """
    Fuses real NDVI data with simulated time-series weather data 
    to create a complete training set.
    """
    # If the database is light, we will bootstrap the data to simulate a larger dataset
    if len(ndvi_list) < 100:
        print("Bootstrapping data to simulate 500 farm plots...")
        ndvi_list = np.random.choice(ndvi_list, 500).tolist()
        
    df = pd.DataFrame({'mean_ndvi': ndvi_list})
    
    # Simulate time-series environmental features (e.g., averages over a growing season)
    np.random.seed(42)
    df['avg_temp_c'] = np.random.normal(25, 3, len(df))       # Average temperature
    df['total_rainfall_mm'] = np.random.normal(400, 50, len(df)) # Seasonal rainfall
    df['soil_moisture_index'] = df['mean_ndvi'] * np.random.uniform(0.8, 1.2, len(df))
    
    # Target Variable: Yield in Tons per Hectare (formula based on ag-science assumptions)
    # Higher NDVI and rainfall generally increase yield, extreme temps reduce it
    base_yield = 2.0
    df['actual_yield_tons_ha'] = (
        base_yield 
        + (df['mean_ndvi'] * 3.5) 
        + (df['total_rainfall_mm'] * 0.005) 
        - (abs(df['avg_temp_c'] - 24) * 0.1) 
        + np.random.normal(0, 0.5, len(df)) # Add some realistic noise
    )
    
    return df

def train_xgboost_model(df):
    """Trains the gradient boosting model to predict crop yield."""
    # Features (X) and Target (y)
    X = df[['mean_ndvi', 'avg_temp_c', 'total_rainfall_mm', 'soil_moisture_index']]
    y = df['actual_yield_tons_ha']
    
    # Split into 80% training and 20% testing data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize the XGBoost Regressor
    print("Training XGBoost Regressor...")
    model = xgb.XGBRegressor(
        objective='reg:squarederror', 
        n_estimators=100, 
        learning_rate=0.1, 
        max_depth=4
    )
    
    # Train the model
    model.fit(X_train, y_train)
    
    # Generate predictions on the test set
    predictions = model.predict(X_test)
    
    # Calculate performance metrics
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    mae = mean_absolute_error(y_test, predictions)
    
    print("\n--- Model Performance Metrics ---")
    print(f"Root Mean Squared Error (RMSE): {rmse:.3f} tons/ha")
    print(f"Mean Absolute Error (MAE):      {mae:.3f} tons/ha")
    print("---------------------------------")
    
    # Feature Importance
    importance = model.feature_importances_
    print("\nFeature Importance:")
    for feature, imp in zip(X.columns, importance):
        print(f"- {feature}: {imp:.2%}")

if __name__ == "__main__":
    print("Connecting to MongoDB and fetching data...")
    raw_ndvi = fetch_mongo_data()
    
    if not raw_ndvi:
        print("No data found in MongoDB. Ensure your Kafka consumer ran successfully.")
    else:
        print(f"Successfully retrieved {len(raw_ndvi)} records from MongoDB.")
        
        # Build the fused dataset
        dataset = generate_training_dataset(raw_ndvi)
        
        # Train the model
        train_xgboost_model(dataset)