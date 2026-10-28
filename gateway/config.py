"""Settings of the gateway, all read from environment variables (set in docker-compose.yml)."""
import os

METADATA_URL = os.getenv("METADATA_URL", "http://127.0.0.1:9000")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/postgres")
ES_URL = os.getenv("ES_URL", "http://127.0.0.1:9200")        # empty string = Elasticsearch switched off
ES_INDEX = os.getenv("ES_INDEX", "objects")

JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production-use-a-long-random-string")
JWT_MINUTES = int(os.getenv("JWT_MINUTES", "60"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
BOOTSTRAP_API_KEY = os.getenv("BOOTSTRAP_API_KEY", "")       # optional fixed API key for the admin (handy for demos)

RATE_LIMIT_PER_MIN = float(os.getenv("RATE_LIMIT_PER_MIN", "600"))   # sustained requests per minute and user
RATE_LIMIT_BURST = int(os.getenv("RATE_LIMIT_BURST", "200"))          # short bursts allowed on top
LOGIN_LIMIT_PER_MIN = float(os.getenv("LOGIN_LIMIT_PER_MIN", "20"))  # login attempts per minute and client address

CORS_ORIGINS = [o for o in os.getenv("CORS_ORIGINS", "*").split(",") if o]
