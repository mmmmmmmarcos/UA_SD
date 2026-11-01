 Sistema Distribuido de Riego 

requiere Docker y Docker Compose instalados
El sistema enruta la mensajería pesada hacia un clúster Kafka remoto. 
IP pública activa: 98.93.69.19.
Si la máquina EC2 se reinicia desde la consola de AWS, su IP cambiará. Si ocurre, actualiza esta dirección en los parámetros `command` del archivo `docker-compose.yml` antes de desplegar.

Sitúate en la raíz del proyecto (donde está el docker-compose.yml). Lanza docker-compose up --build
