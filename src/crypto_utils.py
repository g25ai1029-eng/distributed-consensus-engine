import json
import hashlib
import os
import base64
import logging

log = logging.getLogger("crypto")

try:
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.backends import default_backend
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

KEY_DIR = os.environ.get("KEY_DIR", "/keys")

def generate_keypair(node_id):
    if not CRYPTO_AVAILABLE:
        return
    os.makedirs(KEY_DIR, exist_ok=True)
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    public_key = private_key.public_key()
    priv_path = os.path.join(KEY_DIR, f"node_{node_id}_private.pem")
    with open(priv_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    pub_path = os.path.join(KEY_DIR, f"node_{node_id}_public.pem")
    with open(pub_path, "wb") as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))

def generate_all_keys(num_nodes=6):
    for i in range(1, num_nodes + 1):
        generate_keypair(i)

def load_keys(node_id):
    if not CRYPTO_AVAILABLE:
        return None, {}
    priv_path = os.path.join(KEY_DIR, f"node_{node_id}_private.pem")
    if not os.path.exists(priv_path):
        generate_keypair(node_id)
    with open(priv_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    public_keys = {}
    for i in range(1, 7):
        pub_path = os.path.join(KEY_DIR, f"node_{i}_public.pem")
        if os.path.exists(pub_path):
            with open(pub_path, "rb") as f:
                public_keys[i] = serialization.load_pem_public_key(
                    f.read(), backend=default_backend()
                )
    return private_key, public_keys

def _message_digest(msg):
    msg_copy  = {k: v for k, v in msg.items() if k != "signature"}
    canonical = json.dumps(msg_copy, sort_keys=True)
    return hashlib.sha256(canonical.encode()).digest()

def sign_message(private_key, msg):
    if not CRYPTO_AVAILABLE or private_key is None:
        return base64.b64encode(_message_digest(msg)).decode()
    digest    = _message_digest(msg)
    signature = private_key.sign(
        digest,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode()

def verify_message(public_key, msg):
    if not CRYPTO_AVAILABLE or public_key is None:
        return True
    signature_b64 = msg.get("signature")
    if not signature_b64:
        return False
    try:
        signature = base64.b64decode(signature_b64)
        digest    = _message_digest(msg)
        public_key.verify(
            signature, digest,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256()
        )
        return True
    except Exception:
        return False

if __name__ == "__main__":
    print("Generating keys for all 6 nodes...")
    generate_all_keys(6)
    print(f"Keys written to {KEY_DIR}/")
