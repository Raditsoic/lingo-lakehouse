from fastapi import FastAPI, HTTPException, Depends
from dto import UserRequest
from usecase import fetch_user_data, rank_words
import asyncpg
import os
from dotenv import load_dotenv


load_dotenv()
app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

async def get_pool():
    pool = await asyncpg.create_pool(DATABASE_URL)
    yield pool

async def connect_to_db():
    db = await asyncpg.connect(DATABASE_URL)
    yield db
    await db.close()

@app.on_event("startup")
async def startup():
    app.state.pool = Depends(get_pool)

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

@app.get("/")
async def root():
    return {"message": "Duolingo Birdbrain Api"}

@app.route("/words", methods=["POST"])
async def recommend_words(request: UserRequest, pool: asyncpg.pool = Depends(get_pool)):
    user_id = request.user_id
    
    user_data = await fetch_user_data(pool, user_id)
    if user_data.empty:
        raise HTTPException(status_code=404, detail="User data not found")
    
    # Run ML model
    recall_predictions = inferece(user_data)
    
    # Apply SRA logic
    recommendations = rank_words(recall_predictions)
    
    return recommendations

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)