async def fetch_user_data(db_connection, user_id):
    query = f"SELECT * FROM user_data WHERE user_id = {user_id}"
    return await db_connection.fetch(query)