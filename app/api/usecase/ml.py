import xgboost as xgb
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import os

def load_latest_model(model_path="app/api/models"):
    model_files = [f for f in os.listdir(model_path) if f.startswith('xgboost_model_') and f.endswith('.json')]
    
    if not model_files:
        raise FileNotFoundError("No XGBoost models found in the specified directory")
    
    latest_model_file = sorted(model_files)[-1]
    full_path = os.path.join(model_path, latest_model_file)

    loaded_model = xgb.XGBRegressor()
    loaded_model.load_model(full_path)
    
    print(f"Loaded model: {latest_model_file}")
    return loaded_model

def prepare_new_data(new_data, label_encoders):
    new_data['accuracy_rate'] = new_data['history_correct'] / new_data['history_seen']
    new_data['session_accuracy'] = new_data['session_correct'] / new_data['session_seen']
    new_data['delta_days'] = new_data['delta'] / (60 * 60 * 24)

    categorical_cols = ['user_id', 'learning_language', 'lexeme_id']
    for col in categorical_cols:
        new_data[col] = label_encoders[col].transform(new_data[col])

    new_data = new_data.drop(columns=['delta', 'history_seen', 'history_correct', 'session_seen', 'session_correct'])
    
    return new_data

def inference(model, data):
    return model.predict(data)