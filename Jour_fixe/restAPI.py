import urllib3
import json
from pathlib import Path

# Path
path = Path(__file__).parent

http = urllib3.PoolManager()

# User accounts
f = open(path / "meta" / "account.txt")
lines = f.readlines()

user = "uid98421"  # put your Windows login here
password = lines[0]  # put your secret password here

# request token
token_request_msg = {"user": user, "password": password}
token_request_msg_encoded = json.dumps(token_request_msg).encode("utf-8")

token_request = http.request(
    "POST",
    "https://api.datalake.vitesco.io/token",
    headers={"Content-Type": "application/json"},
    body=token_request_msg_encoded,
)

auth_token = json.loads(token_request.data.decode("utf-8"))
["token"]

# request AWS credentials
credentials_request = http.request(
    "GET",
    "https://api.datalake.vitesco.io/s3/credentials",
    headers={"Content-Type": "application/json", "Authorization": auth_token},
)

credentials = json.loads(credentials_request.data.decode("utf-8"))

aws_access_key_id = credentials["AccessKeyId"]
aws_secret_access_key = credentials["SecretAccessKey"]
aws_session_token = credentials["SessionToken"]
