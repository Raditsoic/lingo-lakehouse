import asyncpg
import os
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from usecase import fetch_user_data, rank_words, inference, load_latest_model_and_tokenizers, prepare_new_data

load_dotenv()
DATABASE_URL = os.getenv("APP_DATABASE_URL")

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(DATABASE_URL)
    app.state.pool = pool
    app.state.model, app.state.tokenizer = load_latest_model_and_tokenizers()
    try:
        yield
    finally:
        await pool.close()

app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:5173",  # React development server 
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"], 
)

async def get_pool():
    return app.state.pool

async def get_model():
    return app.state.model

@app.get("/")
async def root():
    return {"message": "Duolingo Birdbrain API"}

# Recommendation endpoint
@app.get("/words-recommendation")
async def recommend_words(user_id: str = Query(..., description="The ID of the user"), pool: asyncpg.pool = Depends(get_pool)):

    # Fetch user data    
    user_data = await fetch_user_data(pool, user_id)
    if user_data.empty:
        raise HTTPException(status_code=404, detail="User data not found")

    # Prepare new data for model inference
    prepared_data, lexeme_string = prepare_new_data(user_data, app.state.tokenizer)
    
    # Inference
    recall_predictions = inference(prepared_data, lexeme_string, app.state.model)
    
    # Apply SRA logic
    recommendations = rank_words(recall_predictions)
    
    return recommendations

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
