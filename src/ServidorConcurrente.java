import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;

public class ServidorConcurrente {
    // Mapa en memoria que actúa como tabla de estados de las estaciones
    public static ConcurrentHashMap<String, String> estadoEstaciones = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java ServidorConcurrente <puerto_escucha> [broker_kafka]");
            System.exit(1);
        }

        int puerto = Integer.parseInt(args[0]);
        String broker = (args.length >= 2) ? args[1] : "localhost:9092";

        // 1. Arrancar el puente Kafka de Álvaro (peticiones-riego -> ordenes-riego)
        Thread puente = new Thread(new PuenteKafkaCentral(broker));
        puente.start();

        // 2. Arrancar consumidor para recibir los resúmenes finales de WM_WS_E
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
                        System.out.println("\n[CENTRAL-DB] Resumen de riego registrado: " + record.value());
                    }
                }
            } catch (Exception e) {
                System.err.println("[CENTRAL-DB] Error en consumidor de resúmenes: " + e.getMessage());
            }
        }).start();

        // 3. Servidor de Sockets para los Monitores (WM_WS_M)
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