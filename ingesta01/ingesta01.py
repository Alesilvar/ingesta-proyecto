import os
from faker import Faker
import csv
import boto3
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar las variables de entorno
load_dotenv()

# Configuración MongoDB desde variables de entorno
mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/postsdb')  # URL de conexión a MongoDB
db_name = os.getenv('DB_NAME', 'postsdb')  # Nombre de la base de datos
collection_name = 'posts'  # La colección donde se insertarán los posts

# Nombre del CSV de salida
fichero_upload = 'data_posts.csv'

# Bucket S3 (ahora configurado como ingesta01)
nombre_bucket = 'ingesta01'  # Modificado para que use ingesta01

# Instanciar Faker para generar datos falsos
fake = Faker()

# Función para generar 20,000 datos falsos y cargarlos a MongoDB
def generate_and_insert_data():
    # Conexión a MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    for _ in range(20000):
        # Generación de datos ficticios para la colección 'posts'
        post = {
            'title': fake.sentence(nb_words=6),  # Título del post
            'description': fake.text(max_nb_chars=200),  # Descripción del post
            'tags': fake.words(nb=3),  # Etiquetas del post
            'price': fake.random_number(digits=3),  # Precio del post (número aleatorio)
            'presentation_card_id': fake.uuid4(),  # ID de la tarjeta de presentación
            'images': [fake.image_url() for _ in range(3)],  # Array de imágenes (3 imágenes generadas aleatoriamente)
            'is_anonymous': fake.boolean(),  # Anonimato del post
            'created_at': fake.date_this_decade(),  # Fecha de creación del post
            'updated_at': fake.date_this_decade()  # Fecha de última actualización
        }

        # Insertar el post en MongoDB
        collection.insert_one(post)

    client.close()
    print("Datos insertados exitosamente.")

# Función para extraer los datos desde MongoDB y guardarlos en un CSV
def fetch_and_save_data():
    # Conexión a MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Obtener todos los datos de la colección
    rows = list(collection.find())
    columns = rows[0].keys() if rows else []  # Obtener las claves del primer documento para usar como encabezado

    # Guardar en CSV
    with open(fichero_upload, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    client.close()
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
