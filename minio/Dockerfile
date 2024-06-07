# Use a imagem oficial do MinIO como base
FROM minio/minio

# Copie o script de inicialização para o container
COPY init_minio.sh /init_minio.sh

# Conceda permissões de execução ao script de inicialização
RUN chmod +x /init_minio.sh

# Defina o ponto de entrada para executar o script de inicialização
ENTRYPOINT ["/init_minio.sh"]

# Defina o comando para iniciar o servidor MinIO
CMD ["server", "/data"]
