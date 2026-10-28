#!/usr/bin/env python3
"""Throwaway RS256 JWTs for the e2e auth check (e2e/ci/auth.sh); Python standard library + the openssl CLI.

    jwt_make.py jwks <private-key.pem> <jwks.json>
        writes the JWK set (public key, kid "e2e") that the server reads (FLIGHT_JWT_JWKS_FILE)
    jwt_make.py token <private-key.pem> <issuer> <audience> <subject> <lifetime-seconds>
        prints a signed token; a negative lifetime gives an expired one
"""
import base64
import json
import re
import subprocess
import sys
import time

KID = "e2e"


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def int_bytes(n):
    return n.to_bytes((n.bit_length() + 7) // 8, "big")


def jwks(key, out):
    text = subprocess.run(["openssl", "rsa", "-in", key, "-noout", "-text"], check=True, capture_output=True,
                          text=True).stdout
    modulus = re.search(r"modulus:\n((?:\s+[0-9a-f:]+\n)+)", text).group(1)
    n = int(re.sub(r"[\s:]", "", modulus), 16)
    e = int(re.search(r"publicExponent: (\d+)", text).group(1))
    jwk = {"kty": "RSA", "kid": KID, "use": "sig", "alg": "RS256", "n": b64url(int_bytes(n)), "e": b64url(int_bytes(e))}
    with open(out, "w") as f:
        json.dump({"keys": [jwk]}, f)


def token(key, issuer, audience, subject, lifetime):
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT", "kid": KID}
    claims = {"iss": issuer, "aud": audience, "sub": subject, "iat": min(now, now + lifetime),
              "nbf": min(now, now + lifetime) - 5, "exp": now + lifetime}
    signing_input = b64url(json.dumps(header).encode()) + "." + b64url(json.dumps(claims).encode())
    signature = subprocess.run(["openssl", "dgst", "-sha256", "-sign", key], input=signing_input.encode(),
                               check=True, capture_output=True).stdout
    return signing_input + "." + b64url(signature)


if __name__ == "__main__":
    if sys.argv[1] == "jwks":
        jwks(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "token":
        print(token(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6])))
    else:
        sys.exit(__doc__)
