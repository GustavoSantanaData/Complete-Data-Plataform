from fastapi import FastAPI, HTTPException, Depends
from typing import List
from sqlalchemy.orm import Session
from pydantic import BaseModel
from database import SessionLocal, engine
import models
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    'http://localhost:3000',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

class ConnectionBase(BaseModel):
    nome: str
    string: str
    usuario: str
    senha: str

class ConnectionModel(ConnectionBase):
    id: int

    class Config:
        orm_mode = True

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/connections/", response_model=ConnectionModel)
async def create_connection(connection: ConnectionBase, db: Session = Depends(get_db)):
    db_connection = models.Connection(**connection.dict())
    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)
    return db_connection

@app.get("/connections/", response_model=List[ConnectionModel])
async def read_connections(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    connections = db.query(models.Connection).offset(skip).limit(limit).all()
    return connections

models.Base.metadata.create_all(bind=engine)
