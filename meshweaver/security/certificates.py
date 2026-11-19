"""Self-signed certificate generation and SSL context helpers."""

import logging
import ssl
from typing import Any

logger = logging.getLogger("meshweaver.certificates")

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    import datetime
    HAS_X509 = True
except ImportError:
    HAS_X509 = False


def create_self_signed_cert(cert_path: str, key_path: str, common_name: str = "meshweaver.local") -> bool:
    """Generate a self-signed TLS certificate and private key for encrypted P2P channels."""
    if not HAS_X509:
        logger.warning("cryptography library not installed; TLS certificate generation unavailable")
        return False

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MeshWeaver Edge Mesh"),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress_obj) for ipaddress_obj in []]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))

    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    return True


def create_server_ssl_context(cert_path: str, key_path: str) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
    return ctx


def create_client_ssl_context(ca_path: str | None = None, check_hostname: bool = False) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_path)
    if not ca_path:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx.check_hostname = check_hostname
    return ctx
