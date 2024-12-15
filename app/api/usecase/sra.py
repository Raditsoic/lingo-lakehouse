import pandas as pd
from datetime import datetime, timedelta

def calculate_sra(row):
        if row["recall_probability"] > 0.8:
            interval = timedelta(days=7)
        elif row["recall_probability"] > 0.5:
            interval = timedelta(days=3)
        else:
            interval = timedelta(days=1)

        priority = 1 - row["recall_probability"]
        next_review = datetime.now() + interval
        return priority, next_review

# def rank_words(data: pd.DataFrame):
    # data[["priority", "next_review"]] = data.apply(calculate_sra, axis=1, result_type="expand")
    # return data

def rank_words(predictions: pd.DataFrame) -> list:
    try:
        # Sort by predicted recall in descending order
        ranked_words = predictions.sort_values(by='predicted_recall', ascending=True)
        
        # Convert to list of dictionaries for API response
        recommendations = ranked_words[['lexeme_id', 'predicted_recall']].to_dict('records')
        
        return recommendations
    except Exception as e:
        print(f"Error in rank_words: {e}")
        raise    
    
    