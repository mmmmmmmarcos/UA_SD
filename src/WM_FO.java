import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

public class WM_FO {
    private static volatile String estacionActual = "";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java WM_FO <ip_broker:puerto> [id_operador] [ruta_fichero.txt]");
            return;
        }

        String broker = args[0];
        String idOperador = (args.length >= 2 && !args[1].endsWith(".txt")) ? args[1] : "FO-01";
        String rutaFichero = null;
        if (args.length >= 2 && args[1].endsWith(".txt")) {
            rutaFichero = args[1];
        } else if (args.length >= 3) {
            rutaFichero = args[2];
        }

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", broker);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Hilo consumidor: El operario escucha los eventos y resúmenes de la estación que le presta servicio
        new Thread(() -> {
            Properties consProps = new Properties();
            consProps.put("bootstrap.servers", broker);
            consProps.put("group.id", "fo-group-" + idOperador + "-" + UUID.randomUUID());
            consProps.put("auto.offset.reset", "latest");
            consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
                consumer.subscribe(Arrays.asList("ordenes-riego", "resumenes_riego"));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        String val = record.value();
                        if (estacionActual.isEmpty() || val.startsWith(estacionActual)) {
                            System.out.println("\n[NOTIFICACIÓN " + idOperador + "] (" + record.topic() + "): " + val);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[WM_FO] Imposible conectar con WM_Central / Kafka: " + e.getMessage());
            }
        }).start();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            if (rutaFichero != null && !rutaFichero.equalsIgnoreCase("MANUAL")) {
                System.out.println("[WM_FO - " + idOperador + "] Lectura automatizada desde fichero: " + rutaFichero);
                try (BufferedReader br = new BufferedReader(new FileReader(rutaFichero))) {
                    String linea;
                    while ((linea = br.readLine()) != null) {
                        linea = linea.trim();
                        if (linea.isEmpty() || linea.startsWith("#")) continue;

                        String trama = linea.split("#").length == 2 ? linea + "#" + idOperador : linea;
                        estacionActual = trama.split("#")[0];

                        System.out.println("[WM_FO - " + idOperador + "] Solicitando activación: " + trama);
                        producer.send(new ProducerRecord<>("peticiones-riego", estacionActual, trama));
                        producer.flush();

                        System.out.println("[WM_FO] Esperando 4 segundos antes de la siguiente petición...");
                        Thread.sleep(4000);
                    }
                }
            }

            // Modo interactivo para pruebas en laboratorio delante del profesor
            Scanner sc = new Scanner(System.in);
            while (sc.hasNextLine()) {
                System.out.print("\n[" + idOperador + "] Introduce orden (ej. WS-01#5) o 'F' para cargar peticiones.txt: ");
                String entrada = sc.nextLine().trim();
                if (entrada.isEmpty()) continue;

                if (entrada.equalsIgnoreCase("F")) {
                    try (BufferedReader br = new BufferedReader(new FileReader("peticiones.txt"))) {
                        String l;
                        while ((l = br.readLine()) != null) {
                            l = l.trim();
                            if (l.isEmpty() || l.startsWith("#")) continue;
                            String trama = l.split("#").length == 2 ? l + "#" + idOperador : l;
                            estacionActual = trama.split("#")[0];
                            System.out.println("[" + idOperador + "] Enviando de fichero: " + trama);
                            producer.send(new ProducerRecord<>("peticiones-riego", estacionActual, trama));
                            producer.flush();
                            Thread.sleep(4000);
                        }
                    } catch (Exception ex) {
                        System.err.println("[WM_FO] Error leyendo peticiones.txt: " + ex.getMessage());
                    }
                } else {
                    String trama = entrada.split("#").length == 2 ? entrada + "#" + idOperador : entrada;
                    estacionActual = trama.split("#")[0];
                    producer.send(new ProducerRecord<>("peticiones-riego", estacionActual, trama));
                    producer.flush();
                    System.out.println("[" + idOperador + "] Petición enviada: " + trama);
                }
            }
        } catch (Exception e) {
            System.err.println("[WM_FO] Error crítico: Imposible conectar con WM_Central/Broker (" + e.getMessage() + ")");
        }
    }
}