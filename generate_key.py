"""
Run this script once to generate a secure MASTER_ENCRYPTION_KEY.
python generate_key.py
"""
import secrets
import base64

key = secrets.token_bytes(32)
encoded = base64.urlsafe_b64encode(key).decode()
print(f"MASTER_ENCRYPTION_KEY={encoded}")
