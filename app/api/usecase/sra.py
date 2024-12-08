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

def rank_words(data: pd.Dataframe):
    data[["priority", "next_review"]] = data.apply(calculate_sra, axis=1, result_type="expand")
    
    return data