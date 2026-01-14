import base64
import requests
from pathlib import Path

API_URL = "https://api-production-9f0e.up.railway.app/race/upload"
DOWNLOADED_FILE_PATH = "./test_downloaded/race_f4322b83.deflate"

# 1. Load downloaded deflate file
compressed_bytes = Path(DOWNLOADED_FILE_PATH).read_bytes()
print(f"Loaded: {len(compressed_bytes)} bytes")

# 2. Encode to Base64
base64_data = base64.b64encode(compressed_bytes).decode("utf-8")

# 3. Upload
response = requests.post(
    API_URL,
    json={"data": base64_data},
    timeout=60
)

# 4. Result
print("Status:", response.status_code)
print("Response:", response.json())
