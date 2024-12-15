import xgboost as xgb
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import os
import pickle
from typing import Dict, Tuple

def load_latest_model_and_tokenizers(
        model_path: str = "app/api/models", 
        tokenizer_path: str = "app/api/tokenizers"
    ) -> Tuple[xgb.XGBRegressor, Dict[str, LabelEncoder]]:

    model_files = [f for f in os.listdir(model_path) if f.startswith('xgboost_model_') and f.endswith('.json')]
    
    if not model_files:
        raise FileNotFoundError("No XGBoost models found in the specified directory")
    
    # Get the latest model file
    latest_model_file = sorted(model_files)[-1]
    model_full_path = os.path.join(model_path, latest_model_file)

    timestamp = latest_model_file.replace('xgboost_model_', '').replace('.json', '')
    tokenizer_files = [f for f in os.listdir(tokenizer_path) if f.startswith(f'label_encoders_{timestamp}')]
    
    if not tokenizer_files:
        raise FileNotFoundError(f"No matching tokenizers found for model {latest_model_file}")
    
    tokenizer_full_path = os.path.join(tokenizer_path, tokenizer_files[0])

    loaded_model = xgb.XGBRegressor()
    loaded_model.load_model(model_full_path)
    
    with open(tokenizer_full_path, 'rb') as f:
        loaded_label_encoders = pickle.load(f)
    
    print(f"Loaded model from: {model_full_path}")
    print(f"Loaded tokenizers from: {tokenizer_full_path}")
    
    return loaded_model, loaded_label_encoders

def prepare_new_data(new_data: pd.DataFrame, label_encoders: dict) -> pd.DataFrame:
    try:
        new_data['accuracy_rate'] = new_data['history_correct'] / new_data['history_seen']
        new_data['session_accuracy'] = new_data['session_correct'] / new_data['session_seen']
        new_data['delta_days'] = new_data['delta'] / (60 * 60 * 24)

        lexeme_string = new_data['lexeme_string']

        categorical_cols = ['user_id', 'learning_language', 'lexeme_id']
        for col in categorical_cols:
            if col in label_encoders:
                new_data[col] = label_encoders[col].transform(new_data[col].astype(str))
            else:
                raise ValueError(f"LabelEncoder for column '{col}' is missing.")
            
        inference_data = new_data.drop(columns=['delta', 'history_seen', 'history_correct', 'session_seen', 'session_correct', 'lexeme_string', 'timestamp', 'p_recall',])
        
        return inference_data, lexeme_string
    except Exception as e:
        print(f"Error in prepare_new_data: {e}")
        raise

def inference(data: pd.DataFrame, lexeme_string: pd.Series, model) -> pd.DataFrame:
    try:
        predicted_recalls = model.predict(data)
        
        data['predicted_recall'] = predicted_recalls
        
        data['predicted_recall'] = data['predicted_recall'].apply(lambda x: f"{x:.2f}")

        data['lexeme_string'] = lexeme_string.values
        
        return data
    except Exception as e:
        print(f"Error in inference: {e}")
        raise


