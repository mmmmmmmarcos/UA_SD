import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PuenteKafkaCentral implements Runnable {
    private String brokerIp;

    public PuenteKafkaCentral(String brokerIp) {
        this.brokerIp = "52.90.18.232:9092";
    }

    @Override
    public void run() {
        Properties consProps = new Properties();
        consProps.put("bootstrap.servers", this.brokerIp);
        consProps.put("group.id", "central-group");
        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", this.brokerIp);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {

            consumer.subscribe(Collections.singletonList("peticiones-riego"));
            System.out.println("[CENTRAL-KAFKA] Puente iniciado. Escuchando peticiones en: " + this.brokerIp);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String valor = record.value().trim();
                    System.out.println("[CENTRAL-KAFKA] Petición recibida del Operario: " + valor);

                    String idEstacion = "WS-01";
                    String segundosOrden = "5";
                    if (valor.contains("#")) {
                        String[] partes = valor.split("#");
                        idEstacion = partes[0].trim();
                        segundosOrden = partes[1].trim();
                    } else if (valor.startsWith("WS-")) {
                        idEstacion = valor;
                        segundosOrden = "5";
                    } else {
                        segundosOrden = valor;
                    }

                    String estadoBD = GestorBD.obtenerEstadoEstacion(idEstacion);
                    boolean estacionDisponible = "AVAILABLE".equalsIgnoreCase(estadoBD);

                    if (estacionDisponible) {
                        System.out.println("[CENTRAL-KAFKA] SQLite confirma " + idEstacion + " AVAILABLE. Enviando orden (" + segundosOrden + "s)...");
                        producer.send(new ProducerRecord<>("ordenes-riego", idEstacion, segundosOrden));
                    } else {
                        System.out.println("[CENTRAL-KAFKA] Riego denegado para " + idEstacion + ". Estado en SQLite: " + estadoBD);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[CENTRAL-KAFKA] Error crítico en el puente: " + e.getMessage());
        }
    }
}