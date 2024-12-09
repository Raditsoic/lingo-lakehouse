from fastapi import FastAPI, HTTPException, Depends
from dto import UserRequest
from usecase import fetch_user_data, rank_words, inference
import asyncpg
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(DATABASE_URL)
    app.state.pool = pool
    try:
        yield
    finally:
        await pool.close()

app = FastAPI(lifespan=lifespan)

async def get_pool():
    return app.state.pool

@app.get("/")
async def root():
    return {"message": "Duolingo Birdbrain API"}

# Recommendation endpoint
@app.post("/words-recommendation")
async def recommend_words(request: UserRequest, pool: asyncpg.pool = Depends(get_pool)):
    user_id = request.user_id
    
    user_data = await fetch_user_data(pool, user_id)
    if user_data.empty:
        raise HTTPException(status_code=404, detail="User data not found")
    
    # Run ML model
    recall_predictions = inference(user_data)
    
    # Apply SRA logic
    recommendations = rank_words(recall_predictions)
    
    return recommendations

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
