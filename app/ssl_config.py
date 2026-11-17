"""Общие SSL-настройки для Python-клиентов (confluent-kafka / librdkafka, PEM-файлы)."""
from pathlib import Path

CERTS = Path(__file__).resolve().parent.parent / "certs"

BOOTSTRAP = "localhost:9093,localhost:9094,localhost:9095"


def ssl_config(client):
    """client - имя клиентского сертификата: producer | consumer | admin."""
    client_dir = CERTS / "clients" / client
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SSL",
        "ssl.ca.location": str(CERTS / "ca.crt"),
        "ssl.certificate.location": str(client_dir / f"{client}.crt"),
        "ssl.key.location": str(client_dir / f"{client}.key"),
        "ssl.endpoint.identification.algorithm": "https",
    }

