import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PuenteKafkaCentral implements Runnable {
    private String brokerIp;

    public PuenteKafkaCentral(String brokerIp) {
        // Usa la IP real pasada por ServidorConcurrente (AWS EC2)
        this.brokerIp = (brokerIp != null && !brokerIp.isEmpty()) ? brokerIp : "kafka:9092";
    }

    @Override
    public void run() {
        Properties consProps = new Properties();
        consProps.put("bootstrap.servers", this.brokerIp);
        consProps.put("group.id", "central-puente-group");
        consProps.put("auto.offset.reset", "latest");
        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", this.brokerIp);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {

            consumer.subscribe(Collections.singletonList("peticiones-riego"));
            System.out.println("[CENTRAL-KAFKA] Puente activo. Validando peticiones contra SQLite en broker: " + this.brokerIp);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    String valor = record.value().trim();
                    System.out.println("[CENTRAL-KAFKA] Petición recibida: " + valor);

                    String idEstacion = "WS-01";
                    String accion = "5";
                    String operario = "FO-01";

                    if (valor.contains("#")) {
                        String[] partes = valor.split("#");
                        idEstacion = partes[0].trim();
                        accion = partes[1].trim();
                        if (partes.length >= 3) operario = partes[2].trim();
                    } else {
                        accion = valor;
                    }

                    String tramaCompleta = idEstacion + "#" + accion + "#" + operario;

                    // Las órdenes de control prioritario (STOP, FUGA, BLOCK, ACTIVATE) pasan siempre
                    if (accion.equalsIgnoreCase("STOP") || accion.equalsIgnoreCase("FUGA") ||
                        accion.equalsIgnoreCase("BLOCK") || accion.equalsIgnoreCase("ACTIVATE")) {
                        producer.send(new ProducerRecord<>("ordenes-riego", idEstacion, tramaCompleta));
                        producer.flush();
                        continue;
                    }

                    // Para iniciar un riego, validamos en SQLite que esté AVAILABLE (o que no esté en FAULT/Out of Service)
                    String estadoBD = GestorBD.obtenerEstadoEstacion(idEstacion);
                    boolean estacionDisponible = "AVAILABLE".equalsIgnoreCase(estadoBD);

                    if (estacionDisponible) {
                        System.out.println("[CENTRAL-KAFKA] SQLite confirma " + idEstacion + " AVAILABLE. Autorizando riego (" + accion + "s) para " + operario);
                        producer.send(new ProducerRecord<>("ordenes-riego", idEstacion, tramaCompleta));
                        producer.flush();
                    } else {
                        System.out.println("[CENTRAL-KAFKA] Riego DENEGADO para " + idEstacion + " (Solicitante: " + operario + "). Estado actual en SQLite: " + estadoBD);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[CENTRAL-KAFKA] Error crítico en el puente: " + e.getMessage());
        }
    }
}