import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import xgboost as xgb
import datetime
import psycopg2

conn = psycopg2.connect(
    host="localhost",
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
    model_path = "app/api/models"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  
    model_filename = f"{model_path}/xgboost_model_{timestamp}.json" 
    model.save_model(model_filename)
    print(f"Model saved as {model_filename}")
except Exception as e:
    print(f"Error saving model: {e}")

try:
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

    # Preprocess new data
    new_data['accuracy_rate'] = new_data['history_correct'] / new_data['history_seen']
    new_data['session_accuracy'] = new_data['session_correct'] / new_data['session_seen']
    new_data['delta_days'] = new_data['delta'] / (60 * 60 * 24)

    # Encode categorical variables using the label encoders from training
    for col in categorical_cols:
        new_data[col] = label_encoders[col].transform(new_data[col])

    # Drop unnecessary columns
    new_data = new_data.drop(columns=['delta', 'history_seen', 'history_correct', 'session_seen', 'session_correct'])

    predicted_recalls = model.predict(new_data)
    print(f"Predicted Recall Probability: {predicted_recalls[0]:.2f}")

    new_data['predicted_recall'] = predicted_recalls

    ranked_words = new_data.sort_values(by='predicted_recall', ascending=False)
    print(ranked_words[['lexeme_id', 'predicted_recall']])
except Exception as e:
    print(f"Error predicting new data: {e}")

