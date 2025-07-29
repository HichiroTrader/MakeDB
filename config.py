import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Rithmic settings
    RITHMIC_USER = os.getenv('RITHMIC_USER')
    RITHMIC_PASSWORD = os.getenv('RITHMIC_PASSWORD')
    RITHMIC_SYSTEM = os.getenv('RITHMIC_SYSTEM', 'Rithmic Paper Trading')
    
    # Database settings
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))
    DB_NAME = os.getenv('DB_NAME', 'rithmic_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    
    # Redis settings
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    
    # Symbols
    SYMBOLS = os.getenv('SYMBOLS', 'ES:CME,NQ:CME').split(',')
    
    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def redis_url(self):
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}"

config = Config()