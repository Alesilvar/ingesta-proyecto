FROM python:3-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /programas/ingesta

# Instalar las dependencias necesarias
RUN pip3 install --no-cache-dir boto3 pymongo faker python-dotenv

# Copiar todo el contenido del directorio actual al contenedor
COPY . .

# Comando para ejecutar el script de i


CMD ["python3", "./ingesta.py"]
