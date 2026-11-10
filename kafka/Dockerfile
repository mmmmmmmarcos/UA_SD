# Broker Kafka (modo KRaft, un solo nodo) preparado para Railway.
# En Railway: servicio "kafka" con variable RAILWAY_DOCKERFILE_PATH=kafka/Dockerfile,
# volumen montado en /var/lib/kafka/data y un TCP Proxy hacia el puerto 9092.
FROM apache/kafka:3.7.1
USER root
COPY kafka/entrypoint.sh /wm-entrypoint.sh
RUN sed -i 's/\r$//' /wm-entrypoint.sh && chmod +x /wm-entrypoint.sh
EXPOSE 9092 29092
ENTRYPOINT ["/wm-entrypoint.sh"]
