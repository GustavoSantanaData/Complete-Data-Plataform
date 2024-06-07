#!/bin/bash

# Esperar o MinIO iniciar
sleep 12

echo "Iniciando o serviço jedi..."

# Configurar as credenciais de acesso e chave secreta
export MINIO_ACCESS_KEY="DqKP9jCEWxoHZOwMeaha"
export MINIO_SECRET_KEY="pu8AfOYua8KDwfdqUDGuwFfzUb1hPIKbD6aMeWQL"

# Configurar o cliente MinIO
mc alias set myminio http://172.26.0.3:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY --api S3v4

# Criar o bucket
mc mb myminio/prd-dags-jedi