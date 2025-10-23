import boto3
import requests
import random
import logging
import sys
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- CONFIGURACIÓN ----------------
BUCKET_NAME = "ingesta-microservicios-2025"  # bucket original
ANALYTICS_BUCKET = "analytics-microservicios-2025"  # bucket de analytics
REGION_NAME = "us-east-1"
MAX_THREADS = 10
MAX_RETRIES = 3
LOG_FILE = "/tmp/ingesta.log"
BATCH_SIZE = 500

CSV_FILES = {
    "students": "estudiantes.csv",
    "instructores": "instructores.csv",
    "cursos": "cursos.csv",
    "inscripciones": "inscripciones.csv"
}

LOCAL_DIR = "/tmp/ingesta_data"
import os
os.makedirs(LOCAL_DIR, exist_ok=True)

# Cliente S3
s3 = boto3.client("s3", region_name=REGION_NAME)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

MS_ENDPOINTS = {
    "students": "http://LB-Microservicios-34846879.us-east-1.elb.amazonaws.com/estudiantes",
    "cursos": "http://LB-Microservicios-34846879.us-east-1.elb.amazonaws.com/cursos",
    "instructores": "http://LB-Microservicios-34846879.us-east-1.elb.amazonaws.com/instructores",
    "inscripciones": "http://LB-Microservicios-34846879.us-east-1.elb.amazonaws.com/inscripciones"
}

# ---------- FUNCIONES ----------

def download_csvs():
    logging.info(f"Descargando archivos desde S3 ({BUCKET_NAME})")
    for ms, filename in CSV_FILES.items():
        local_path = os.path.join(LOCAL_DIR, filename)
        try:
            s3.download_file(BUCKET_NAME, filename, local_path)
            logging.info(f"{filename} descargado correctamente")
        except Exception as e:
            logging.error(f"No se pudo descargar {filename}: {e}")

def post_with_retries(url, json_data):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, json=json_data, timeout=5)
            if response.status_code in [200, 201]:
                logging.info(f"Registro enviado correctamente: {json_data}")
                return response.json()
            else:
                logging.warning(f"Error {response.status_code} enviando: {json_data}")
        except Exception as e:
            logging.error(f"Falló el envío: {json_data} | Error: {e}")
        time.sleep(0.5 * (attempt + 1))
    return None

def fetch_student_ids():
    """Obtiene IDs de estudiantes paginados desde el microservicio"""
    student_ids = []
    page = 0
    size = 100
    while True:
        try:
            resp = requests.get(f"{MS_ENDPOINTS['students']}?page={page}&size={size}", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            content = data.get("content", [])
            student_ids.extend([s["id"] for s in content])
            if data.get("last", True):
                break
            page += 1
        except Exception as e:
            logging.error(f"No se pudo obtener estudiantes: {e}")
            break
    logging.info(f"Total estudiantes obtenidos: {len(student_ids)}")
    return student_ids

def generate_inscripcion(student_ids, n_inscripciones=20000):
    for _ in range(n_inscripciones):
        estudiante_id = random.choice(student_ids)
        curso_id = random.randint(1, 20000)  # curso aleatorio
        estado = random.choice(["activa", "completada", "cancelada"])
        metodo_pago = random.choice(["tarjeta", "paypal", "transferencia"])
        monto = round(random.uniform(50, 500), 2)
        total_lecciones = random.randint(5, 20)
        lecciones_completadas = random.sample(range(1, total_lecciones+1), random.randint(0, total_lecciones))
        progreso = {
            "porcentaje": round(len(lecciones_completadas)/total_lecciones*100, 2),
            "leccionesCompletadas": lecciones_completadas,
            "ultimaLeccionId": max(lecciones_completadas) if lecciones_completadas else 0
        }
        fecha_inscripcion = (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        yield {
            "estudianteId": estudiante_id,
            "cursoId": curso_id,
            "estado": estado,
            "metodoPago": metodo_pago,
            "monto": monto,
            "progreso": progreso,
            "fechaInscripcion": fecha_inscripcion
        }

def print_progress(current, total, prefix='Progreso'):
    percent = current / total * 100
    bar_len = 40
    filled_len = int(bar_len * current // total)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write(f'\r{prefix}: |{bar}| {percent:.1f}% ({current}/{total})')
    sys.stdout.flush()
    if current == total:
        print()

def generate_and_send_inscripciones():
    student_ids = fetch_student_ids()
    if not student_ids:
        logging.warning("No hay estudiantes en la base para generar inscripciones")
        return
    n_inscripciones = 20000
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []
        for i, insc in enumerate(generate_inscripcion(student_ids, n_inscripciones), 1):
            futures.append(executor.submit(post_with_retries, MS_ENDPOINTS["inscripciones"], insc))
            if i % BATCH_SIZE == 0:
                print_progress(i, n_inscripciones, prefix="Ingesta de inscripciones")
        for future in as_completed(futures):
            future.result()
    print_progress(n_inscripciones, n_inscripciones, prefix="Ingesta de inscripciones")

# ---------- COPIA A BUCKET ANALYTICS ----------

def copy_csvs_to_analytics(source_bucket, target_bucket, csv_files):
    today = datetime.now().strftime("%Y-%m-%d")
    for key in csv_files.values():
        try:
            source_key = key
            dest_key = f"{key.split('.')[0]}/{today}/{key}"
            s3.copy_object(
                Bucket=target_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=dest_key
            )
            logging.info(f"{source_key} copiado a analytics como {dest_key}")
        except Exception as e:
            logging.error(f"No se pudo copiar {source_key} a analytics: {e}")

# ---------- MAIN ----------

def main():
    logging.info("===== INICIO DE INGESTA =====")
    download_csvs()

    # ---------- COMENTADO PARA AHORRAR TIEMPO ----------
    # for ms in ["students", "instructores", "cursos"]:
    #     filename = CSV_FILES[ms]
    #     local_path = os.path.join(LOCAL_DIR, filename)
    #     if os.path.exists(local_path):
    #         send_data_to_ms(ms, local_path)
    #     else:
    #         logging.warning(f"Saltando {ms}: {filename} no encontrado")

    # Generar inscripciones usando IDs de DB y curso aleatorio
    generate_and_send_inscripciones()

    # Copiar todos los CSV al bucket de Analytics
    copy_csvs_to_analytics(BUCKET_NAME, ANALYTICS_BUCKET, CSV_FILES)

    logging.info("===== INGESTA FINALIZADA =====")
    print("\n✅ Ingesta completada. Logs en:", LOG_FILE)

if __name__ == "__main__":
    main()
