import os
import redis
from rq import Queue

# Redis connection
redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    db=int(os.getenv("REDIS_DB", "0")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=False  # Keep binary data as bytes
)

# Create RQ queue for race processing
race_queue = Queue("race_processing", connection=redis_conn)

def get_race_queue() -> Queue:
    """Get the race processing queue."""
    return race_queue
