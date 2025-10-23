import boto3
import csv
import os
import requests
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
import math
import sys

# ---------------- CONFIGURACIÓN ----------------
BUCKET_NAME = "ingesta-microservicios-2025"
REGION_NAME = "us-east-1"
MAX_THREADS = 10
MAX_RETRIES = 3
LOG_FILE = "/tmp/ingesta.log"
BATCH_SIZE = 500
# -----------------------------------------------

# Configuración de logging
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

CSV_FILES = {
    "students": "estudiantes.csv",
    "instructores": "instructores.csv",
    "cursos": "cursos.csv",
}

LOCAL_DIR = "/tmp/ingesta_data"
os.makedirs(LOCAL_DIR, exist_ok=True)

s3 = boto3.client("s3", region_name=REGION_NAME)

# ---------- IDs generados ----------
STUDENTS_IDS = []
INSTRUCTORES_IDS = {}
CURSOS_IDS = []

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

def send_data_to_ms(ms_name, csv_path):
    endpoint = MS_ENDPOINTS[ms_name]
    logging.info(f"Iniciando ingesta para {ms_name} ({endpoint})")

    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {}
            for row in reader:
                # ---------- Ajustes de tipo ----------
                if ms_name == "cursos":
                    if 'duracion_min' in row:
                        try:
                            row['duracion_min'] = int(row['duracion_min'])
                        except ValueError:
                            row['duracion_min'] = 0

                    if 'instructor_ids' in row:
                        # Remplazar instructor_ids por los IDs reales
                        instr = row['instructor_ids']
                        instr_list = []
                        if isinstance(instr, str):
                            for name in instr.split(','):
                                name = name.strip()
                                if name in INSTRUCTORES_IDS:
                                    instr_list.append(INSTRUCTORES_IDS[name])
                        row['instructor_ids'] = instr_list

                futures[executor.submit(post_with_retries, endpoint, row)] = row

            for future in as_completed(futures):
                result = future.result()
                if result:
                    if ms_name == "students":
                        STUDENTS_IDS.append(result.get("id"))
                    elif ms_name == "instructores":
                        # Guardamos mapping nombre → id
                        key = row.get("nombre") or f"id_{len(INSTRUCTORES_IDS)+1}"
                        INSTRUCTORES_IDS[key] = result.get("id")
                    elif ms_name == "cursos":
                        CURSOS_IDS.append(result.get("id"))

def generate_inscripcion():
    estudiante_id = random.choice(STUDENTS_IDS)
    curso_id = random.choice(CURSOS_IDS)
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
    return {
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

def generate_and_send_inscripciones(n_inscripciones=20000):
    if not STUDENTS_IDS or not CURSOS_IDS:
        logging.warning("No hay estudiantes o cursos válidos para generar inscripciones")
        return

    logging.info(f"Generando y enviando {n_inscripciones} inscripciones en batches de {BATCH_SIZE}")
    total_batches = math.ceil(n_inscripciones / BATCH_SIZE)

    for batch_num in range(total_batches):
        current_batch_size = min(BATCH_SIZE, n_inscripciones - batch_num * BATCH_SIZE)
        batch = [generate_inscripcion() for _ in range(current_batch_size)]

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(post_with_retries, MS_ENDPOINTS["inscripciones"], insc) for insc in batch]
            for future in as_completed(futures):
                future.result()

        print_progress(batch_num + 1, total_batches, prefix='Ingesta de inscripciones')

# ---------- MAIN ----------

def main():
    logging.info("===== INICIO DE INGESTA =====")
    download_csvs()

    # ---------- ORDEN CORRECTO ----------
    for ms in ["students", "instructores", "cursos"]:
        filename = CSV_FILES[ms]
        local_path = os.path.join(LOCAL_DIR, filename)
        if os.path.exists(local_path):
            send_data_to_ms(ms, local_path)
        else:
            logging.warning(f"Saltando {ms}: {filename} no encontrado")

    generate_and_send_inscripciones()
    logging.info("===== INGESTA FINALIZADA =====")
    print("\n✅ Ingesta completada. Logs en:", LOG_FILE)

if __name__ == "__main__":
    main()
