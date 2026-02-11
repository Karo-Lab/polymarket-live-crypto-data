import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        self.TOPIC = os.getenv("TOPIC")
        self.GAMMA_ENDPOINT = os.getenv(
            "GAMMA_ENDPOINT", "https://gamma-api.polymarket.com/events/slug"
        )
        self.PRE_WARM_TIME = 45
        self.WS_URL = os.getenv("WS_URL", "wss://ws-subscriptions-clob.polymarket.com")
        self.ALL_PROXY = os.getenv("ALL_PROXY")


config = Config()


class QuestDBConfig:
    def __init__(self) -> None:
        host = os.getenv("QUEST_DB_HOST", "localhost")
        port = int(os.getenv("QUEST_DB_PORT", 9000))
        self.url = f"http::addr={host}:{port};"

        pwp_port = int(os.getenv("QUEST_DB_PWP", 8812))
        self.pwp = f"user=admin password=quest host={host} port={pwp_port} dbname=qdb"


quest_db_cfg = QuestDBConfig()


class PostgresDBConfig:
    def __init__(self) -> None:
        host = os.getenv("POSTGRES_DB_HOST", "localhost")
        port = int(os.getenv("POSTGRES_DB_PORT", 5432))
        user = os.getenv("POSTGRES_DB_USER", "postgres")
        password = os.getenv("POSTGRES_DB_PASSWORD", "kms")
        db_name = os.getenv("POSTGRES_DB_NAME", "postgres")

        self.url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


postgres_db_cfg = PostgresDBConfig()
