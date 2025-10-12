from sqlalchemy import create_engine, Boolean, Column, Integer, String, Float, ForeignKey, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://bball_user:bball_password@db:5432/basketball_stats"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Player(Base):
    __tablename__ = "players"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    player_id = Column(String, unique=True, index=True)  # Basketball Reference ID
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    game_stats = relationship("GameStats", back_populates="player")
    elo_stats = relationship("EloStats", back_populates="player", uselist=False) # One-to-one

class GameStats(Base):
    __tablename__ = "game_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    game_date = Column(Date, nullable=False, index=True)
    season = Column(Integer, nullable=False, index=True)
    team = Column(String, index=True)
    home_away = Column(String)  # '@' for away, blank for home
    opponent = Column(String, index=True)
    game_result = Column(String)  # 'W' or 'L'
    game_score = Column(String)  # e.g., "120-115"
    game_started = Column(Integer)  # 1 if started, 0 if bench
    minutes_played = Column(String)  # Can be "00:00" format or empty
    field_goals = Column(Integer)
    field_goal_attempts = Column(Integer)
    field_goal_pct = Column(Float)
    three_pointers = Column(Integer)
    three_point_attempts = Column(Integer)
    three_point_pct = Column(Float)
    free_throws = Column(Integer)
    free_throw_attempts = Column(Integer)
    free_throw_pct = Column(Float)
    offensive_rebounds = Column(Integer)
    defensive_rebounds = Column(Integer)
    total_rebounds = Column(Integer)
    assists = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    personal_fouls = Column(Integer)
    points = Column(Integer)
    game_score_stat = Column(Float)  # Basketball Reference's Game Score metric
    plus_minus = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    player = relationship("Player", back_populates="game_stats")

class EloStats(Base):
    """
    Stores the overall and category-specific ELO ratings for each player.
    """
    __tablename__ = "elo_stats"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, unique=True)
    
    # ELO ratings
    overall_elo = Column(Float, nullable=False, default=1500)
    fg_pct_elo = Column(Float, nullable=False, default=1500)
    ft_pct_elo = Column(Float, nullable=False, default=1500)
    pts_elo = Column(Float, nullable=False, default=1500)
    reb_elo = Column(Float, nullable=False, default=1500)
    ast_elo = Column(Float, nullable=False, default=1500)
    stl_elo = Column(Float, nullable=False, default=1500)
    blk_elo = Column(Float, nullable=False, default=1500)
    to_elo = Column(Float, nullable=False, default=1500) # Turnovers
    tpm_elo = Column(Float, nullable=False, default=1500) # Three-Pointers Made
    overall_elo_guaranteed = Column(Float, nullable=False, default=1500)
    fg_pct_elo_guaranteed = Column(Float, nullable=False, default=1500)
    ft_pct_elo_guaranteed = Column(Float, nullable=False, default=1500)
    pts_elo_guaranteed = Column(Float, nullable=False, default=1500)
    reb_elo_guaranteed = Column(Float, nullable=False, default=1500)
    ast_elo_guaranteed = Column(Float, nullable=False, default=1500)
    stl_elo_guaranteed = Column(Float, nullable=False, default=1500)
    blk_elo_guaranteed = Column(Float, nullable=False, default=1500)
    to_elo_guaranteed = Column(Float, nullable=False, default=1500) # Turnovers
    tpm_elo_guaranteed = Column(Float, nullable=False, default=1500) # Three-Pointers Made
    dropped_player = Column(Boolean, nullable=False, default=False) # Whether player is removed from simulations.
    
    # Relationship
    player = relationship("Player", back_populates="elo_stats")

class SimulationInfo(Base):
    """
    A simple table to store metadata about the simulation, like how many have been run.
    We assume only one row will exist in this table.
    """
    __tablename__ = "simulation_info"
    id = Column(Integer, primary_key=True, index=True)
    simulation_count = Column(Integer, default=0, nullable=False)


def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
    print("Database tables created successfully!")