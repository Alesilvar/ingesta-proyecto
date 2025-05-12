import os
import mysql.connector
from faker import Faker
import csv
import boto3

# Configuración MySQL desde variables de entorno
db_host = os.getenv('MYSQL_HOST')
db_port = int(os.getenv('MYSQL_PORT', '3306'))
db_user = os.getenv('MYSQL_USER')
db_password = os.getenv('MYSQL_PASSWORD')
db_name = os.getenv('MYSQL_DATABASE')
table_name = os.getenv('MYSQL_TABLE', 'purchases')  # Cambia a la tabla que desees

# Nombre del CSV de salida
fichero_upload = 'data_mysql.csv'

# Bucket S3 (por defecto o desde variable de entorno)
nombre_bucket = os.getenv('S3_BUCKET', '')

# Instanciar Faker para generar datos falsos
fake = Faker()

# Función para generar 20,000 datos falsos y cargarlos a MySQL
def generate_and_insert_data():
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    cursor = conn.cursor()
    
    for _ in range(20000):
        # Generación de datos ficticios para la tabla 'purchases'
        buyer_id = fake.uuid4()
        post_id = fake.uuid4()
        seller_confirmed_at = fake.date_time_this_year()
        is_inactive = fake.boolean()
        created_at = fake.date_time_this_year()
        updated_at = fake.date_time_this_year()

        # SQL de inserción
        insert_query = f"""
            INSERT INTO {table_name} (id, buyer_id, post_id, seller_confirmed_at, is_inactive, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (fake.uuid4(), buyer_id, post_id, seller_confirmed_at, is_inactive, created_at, updated_at))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Datos insertados exitosamente.")

# Función para extraer los datos desde MySQL y guardarlos en un CSV
def fetch_and_save_data():
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    conn.close()

    with open(fichero_upload, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    return fichero_upload

# Función para subir el archivo CSV a S3
def upload_to_s3(file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, nombre_bucket, file_path)
    print(f"Ingesta completada: {file_path} subido a s3://{nombre_bucket}/{file_path}")

if __name__ == '__main__':
    generate_and_insert_data()  # Generar e insertar datos
    csv_file = fetch_and_save_data()  # Obtener datos y guardarlos en CSV
    upload_to_s3(csv_file)  # Subir CSV a S3
