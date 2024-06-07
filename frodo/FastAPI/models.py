from database import Base
from sqlalchemy import Column, Integer, String, Boolean, Float


class Connection(Base):
    __tablename__ = 'connections'

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String)
    string = Column(String)
    usuario = Column(String)
    senha = Column(String)