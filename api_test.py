import base64
import requests
from pathlib import Path

API_URL = "https://api-production-9f0e.up.railway.app/race/upload"
DEFLECTED_FILE_PATH = "./mock_storage/5dd023c6-519a-4618-b3e4-4c2933f7ebc4.deflate"

# 1. Load raw deflate bytes
compressed_bytes = Path(DEFLECTED_FILE_PATH).read_bytes()

# 2. Encode to Base64 (string)
base64_data = base64.b64encode(compressed_bytes).decode("utf-8")

# Optional safety check (recommended)
assert compressed_bytes == base64.b64decode(base64_data)

# 3. Build request payload
payload = {
    "data": base64_data
}

# 4. Send request
response = requests.post(
    API_URL,
    json=payload,
    timeout=60
)

# 5. Inspect response
print("Status:", response.status_code)
print("Response:", response.json())
