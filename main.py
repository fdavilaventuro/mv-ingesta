import boto3
import csv
import os
import requests

# Configuración general
BUCKET_NAME = "tu-bucket-s3-ingesta"  # ⚠️ reemplaza con tu bucket real
REGION_NAME = "us-east-1"

# Endpoints de microservicios
MS_ENDPOINTS = {
    "students": "http://<IP_BALANCEADOR>/estudiantes",
    "cursos": "http://<IP_BALANCEADOR>/cursos",
    "inscripciones": "http://<IP_BALANCEADOR>/inscripciones",
    "agregador": "http://<IP_BALANCEADOR>/agregador"
}

# Archivos CSV esperados en el bucket
CSV_FILES = {
    "students": "students.csv",
    "cursos": "cursos.csv",
    "inscripciones": "inscripciones.csv",
    "agregador": "agregador.csv"
}

# Directorio local temporal
LOCAL_DIR = "/tmp/ingesta_data"
os.makedirs(LOCAL_DIR, exist_ok=True)

# Cliente S3
s3 = boto3.client("s3", region_name=REGION_NAME)

def download_csvs():
    print(f"📥 Descargando archivos desde S3 ({BUCKET_NAME})...")
    for ms, filename in CSV_FILES.items():
        local_path = os.path.join(LOCAL_DIR, filename)
        try:
            s3.download_file(BUCKET_NAME, filename, local_path)
            print(f"✅ {filename} descargado correctamente.")
        except Exception as e:
            print(f"⚠️ No se pudo descargar {filename}: {e}")

def send_data_to_ms(ms_name, csv_path):
    endpoint = MS_ENDPOINTS[ms_name]
    print(f"\n📤 Iniciando ingesta para {ms_name} ({endpoint})")

    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                response = requests.post(endpoint, json=row, timeout=5)
                if response.status_code in [200, 201]:
                    print(f"  → Registro enviado correctamente: {row}")
                else:
                    print(f"  ⚠️ Error {response.status_code} enviando {row}")
            except Exception as e:
                print(f"  ❌ Falló el envío de {row}: {e}")

def main():
    download_csvs()
    for ms, filename in CSV_FILES.items():
        local_path = os.path.join(LOCAL_DIR, filename)
        if os.path.exists(local_path):
            send_data_to_ms(ms, local_path)
        else:
            print(f"⚠️ Saltando {ms}: {filename} no encontrado.")

if __name__ == "__main__":
    main()
