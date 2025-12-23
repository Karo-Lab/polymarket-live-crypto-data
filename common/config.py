from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    def __init__(self):
        # self.ROTATION_INTERVAL: int = int(os.getenv("ROTATION_INTERVAL", 900))
        self.TOPIC = os.getenv("TOPIC")
        self.GAMMA_ENDPOINT = os.getenv("GAMMA_ENDPOINT", "https://gamma-api.polymarket.com/events/slug") 
        self.PRE_WARM_TIME = 45      
        self.WS_URL = os.getenv("WS_URL","wss://ws-subscriptions-clob.polymarket.com")
        
config = Config()

class QuestDBConfig:
    def __init__(self) -> None:
        host = os.getenv("QUEST_DB_HOST", "localhost")
        port = int(os.getenv("QUEST_DB_PORT", 9000))
        self.url = f"http::addr={host}:{port};"
        
    
quest_db_cfg = QuestDBConfig()