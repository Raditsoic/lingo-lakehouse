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
    # Compute derived features
    new_data['accuracy_rate'] = new_data['history_correct'] / new_data['history_seen']
    new_data['session_accuracy'] = new_data['session_correct'] / new_data['session_seen']
    new_data['delta_days'] = new_data['delta'] / (60 * 60 * 24)

    # Encode categorical columns
    categorical_cols = ['user_id', 'learning_language', 'lexeme_id']
    for col in categorical_cols:
        if col in label_encoders:
            new_data[col] = label_encoders[col].transform(new_data[col])
        else:
            raise ValueError(f"LabelEncoder for column '{col}' is missing.")

    # Drop unnecessary columns
    new_data = new_data.drop(columns=['delta', 'history_seen', 'history_correct', 'session_seen', 'session_correct'])
    
    return new_data

def inference(model, data):
    return model.predict(data)

# Load the model
model = load_latest_model()

# Fit LabelEncoders with mock data (in production, use the training data for fitting)
label_encoders = {
    'user_id': LabelEncoder().fit(['u:FO', 'u:BA']),  # Example training categories
    'learning_language': LabelEncoder().fit(['de', 'fr']),
    'lexeme_id': LabelEncoder().fit(['76390c1350a8dac31186187e2fe1e178', '7dfd7086f3671685e2cf1c1da72796d7'])
}

# Example new data
new_data = pd.DataFrame({
    'delta': [3600, 7200],
    'user_id': ['u:FO', 'u:FO'],
    'learning_language': ['de', 'de'],
    'lexeme_id': ['76390c1350a8dac31186187e2fe1e178', '7dfd7086f3671685e2cf1c1da72796d7'],
    'history_seen': [8, 6],
    'history_correct': [6, 4],
    'session_seen': [3, 2],
    'session_correct': [2, 1],
})

# Prepare the data for prediction
new_data_prepared = prepare_new_data(new_data, label_encoders)

# XGBoost requires data as a NumPy array
predictions = inference(model, new_data_prepared.values)

results = pd.DataFrame({
    'user_id': new_data['user_id'],
    'lexeme_id': new_data['lexeme_id'],
    'predicted_recall': predictions
})

print(results)
