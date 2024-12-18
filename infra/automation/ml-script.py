import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import xgboost as xgb
import datetime
import psycopg2
import pickle
import os

os.makedirs("app/api/models", exist_ok=True)
os.makedirs("app/api/tokenizers", exist_ok=True)

conn = psycopg2.connect(
    host="postgres-warehouse",
    database="mldb",
    user="soic",
    password="123"
)

data = pd.read_sql("SELECT * FROM ml_data", conn)

print(data.head())

# Encoding categorical features
categorical_cols = ['user_id', 'learning_language', 'lexeme_id']
label_encoders = {}

for col in categorical_cols:
    le = LabelEncoder()
    data[col] = le.fit_transform(data[col])
    label_encoders[col] = le

# Prepare training data
target_col = 'p_recall'
X = data.drop(columns=[target_col])
y = data[target_col]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train XGBoost model
model = xgb.XGBRegressor(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    objective='reg:squarederror',
    random_state=42
)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Print predictions
for true, pred in zip(y_test[:5], y_pred[:5]):
    print(f"True Recall: {true:.2f}, Predicted Recall: {pred:.2f}")

# Save the model with a timestamped filename
try:
    model_path = "/local/models"
    tokenizer_path = "/local/tokenizers"

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  
    model_filename = f"{model_path}/xgboost_model_{timestamp}.json" 
    model.save_model(model_filename)
    print(f"Model saved as {model_filename}")

    tokenizer_filename = f"{tokenizer_path}/label_encoders_{timestamp}.pkl"
    with open(tokenizer_filename, 'wb') as f:
        pickle.dump(label_encoders, f)
    print(f"Label encoders saved as {tokenizer_filename}")
except Exception as e:
    print(f"Error saving model: {e}")

