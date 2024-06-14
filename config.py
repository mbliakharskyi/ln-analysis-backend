import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    UPLOAD_FOLDER = 'uploads'
    PILOTERR_API_URL = 'https://piloterr.com/api/v2/linkedin/profile/info'
    PILOTERR_API_KEY = os.environ.get('API_KEY')
    RATE_LIMIT = 7  # requests per second
    REQUEST_INTERVAL = 1 / RATE_LIMIT  # interval between requests


class DevelopmentConfig(Config):
    DEBUG = True
    ENV = 'development'


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
    ENV = 'testing'


class ProductionConfig(Config):
    DEBUG = False
    ENV = 'production'


config_by_name = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig
}