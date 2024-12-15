import pandas as pd

async def fetch_user_data(db_connection, user_id):
    query = "SELECT * FROM app_data WHERE user_id = $1;"
    print(f"Executing query: {query} with user_id={user_id}")

    records = await db_connection.fetch(query, user_id)

    if not records:
        return None  
    
    column_names = list(records[0].keys())
    
    data_dicts = [dict(record) for record in records]

    data_df = pd.DataFrame(data_dicts, columns=column_names)
    data_df = data_df.drop_duplicates(subset=['lexeme_id'], keep='first')
    
    return data_df
