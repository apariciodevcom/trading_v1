# /home/ubuntu/tr/my_modules/email_sender.py

import boto3
import os
import logging
from my_modules.config import MAX_EMAILS_PER_DAY, SES_PROFILE_NAME, LOCAL_LOG_PATH

SENDER_EMAIL = os.getenv("EMAIL_TRADING")

if not SENDER_EMAIL:
    raise ValueError("La variable de entorno EMAIL_TRADING no está definida.")

def enviar_email(asunto, cuerpo, destinatario, adjuntos=None, html=False):
    session = boto3.Session(profile_name=SES_PROFILE_NAME)
    ses = session.client("ses", region_name="eu-central-1")

    if html:
        body = {"Html": {"Data": cuerpo}}
    else:
        body = {"Text": {"Data": cuerpo}}

    msg = {
        "Subject": {"Data": asunto},
        "Body": body
    }

    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [destinatario]},
            Message=msg
        )
        logging.info(f"Email enviado exitosamente: {response['MessageId']}")
        return True
    except Exception as e:
        logging.error(f"Error al enviar email: {str(e)}")
        return False

def puede_enviar_mas_emails(contador_actual):
    return contador_actual < MAX_EMAILS_PER_DAY

if __name__ == "__main__":
    log_path = os.path.join(LOCAL_LOG_PATH, "email_sender.log")

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    logging.info(f"Test de envío activado con símbolos: {SYMBOLS}")
    logging.info(f"Remitente configurado: {SENDER_EMAIL}")
