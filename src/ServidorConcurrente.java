import java.net.*;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;

public class ServidorConcurrente {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java ServidorConcurrente <puerto_escucha> [broker_kafka]");
            System.exit(1);
        }

        int puerto = Integer.parseInt(args[0]);
        String broker = (args.length >= 2) ? args[1] : "localhost:9092";

        // 1. Inicializar SQLite
        GestorBD.inicializarBD();

        // 2. Arrancar el puente Kafka (peticiones-riego -> ordenes-riego)
        Thread puente = new Thread(new PuenteKafkaCentral(broker));
        puente.start();

        // 3. Consumidor para guardar resúmenes de riego en SQLite
        new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", broker);
            props.put("group.id", "central-resumenes-group");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList("resumenes_riego"));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        GestorBD.registrarResumen(record.value());
                    }
                }
            } catch (Exception e) {
                System.err.println("[CENTRAL-DB] Error en consumidor de resúmenes: " + e.getMessage());
            }
        }).start();

        // 4. Servidor de Sockets para los Monitores (WM_WS_M)
        try (ServerSocket skServidor = new ServerSocket(puerto)) {
            System.out.println("[WM_CENTRAL] Escuchando monitores en el puerto " + puerto + "...");
            for (;;) {
                Socket skCliente = skServidor.accept();
                System.out.println("[WM_CENTRAL] Nuevo monitor conectado desde " + skCliente.getInetAddress());
                new HiloServidor(skCliente).start();
            }
        } catch (Exception e) {
            System.out.println("[WM_CENTRAL] Error: " + e.toString());
        }
    }
}