# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv 
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras

SECRET_KEY = os.getenv("SECRET_KEY", "fallback_pour_le_dev")
ALGORITHM  = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

USERS = {
    "admin": {
        "username": "admin",
        "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW"
    }
}

app = FastAPI(title="Spotify Data API", version="1.0")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_db():
    return psycopg2.connect(**DB_CONFIG)

def create_token(data: dict):
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    data.update({"exp": expire})
    return jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username not in USERS:
            raise HTTPException(status_code=401, detail="Token invalide")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalide")

#Auth
@app.post("/token")
def login(form: OAuth2PasswordRequestForm = Depends()):
    user = USERS.get(form.username)
    if not user or not pwd_context.verify(form.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    token = create_token({"sub": form.username})
    return {"access_token": token, "token_type": "bearer"}

@app.get("/")
def root():
    return {"message": "Spotify Data API", "status": "ok"}

@app.get("/audio-by-decade")
def audio_by_decade(
    current_user: str = Depends(get_current_user),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    offset = (page - 1) * size
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) as total FROM dm_audio_by_decade")
    total = cur.fetchone()["total"]
    cur.execute("SELECT * FROM dm_audio_by_decade ORDER BY decade LIMIT %s OFFSET %s", (size, offset))
    rows = cur.fetchall()
    conn.close()
    return {"page": page, "size": size, "total": total, "data": rows}

@app.get("/top-tracks")
def top_tracks(
    current_user: str = Depends(get_current_user),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    offset = (page - 1) * size
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) as total FROM dm_top_tracks")
    total = cur.fetchone()["total"]
    cur.execute("SELECT * FROM dm_top_tracks ORDER BY popularity DESC LIMIT %s OFFSET %s", (size, offset))
    rows = cur.fetchall()
    conn.close()
    return {"page": page, "size": size, "total": total, "data": rows}

@app.get("/genre-popularity")
def genre_popularity(
    current_user: str = Depends(get_current_user),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    offset = (page - 1) * size
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) as total FROM dm_genre_popularity")
    total = cur.fetchone()["total"]
    cur.execute("SELECT * FROM dm_genre_popularity ORDER BY avg_popularity DESC LIMIT %s OFFSET %s", (size, offset))
    rows = cur.fetchall()
    conn.close()
    return {"page": page, "size": size, "total": total, "data": rows}

@app.get("/top-artists")
def top_artists(
    current_user: str = Depends(get_current_user),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    offset = (page - 1) * size
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) as total FROM dm_top_artists")
    total = cur.fetchone()["total"]
    cur.execute("SELECT * FROM dm_top_artists ORDER BY artist_popularity DESC LIMIT %s OFFSET %s", (size, offset))
    rows = cur.fetchall()
    conn.close()
    return {"page": page, "size": size, "total": total, "data": rows}