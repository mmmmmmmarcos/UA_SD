import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PuenteKafkaCentral implements Runnable {
    private String brokerIp;

    public PuenteKafkaCentral(String brokerIp) {
        this.brokerIp = brokerIp;
    }

    @Override
    public void run() {
        Properties consProps = new Properties();
        consProps.put("bootstrap.servers", brokerIp);
        consProps.put("group.id", "central-group");
        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", brokerIp);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {

            consumer.subscribe(Collections.singletonList("peticiones-riego"));
            System.out.println("[CENTRAL-KAFKA] Puente iniciado. Escuchando peticiones en: " + brokerIp);

            while (true) {
                // Leer eventos cada segundo
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("[CENTRAL-KAFKA] Petición recibida del Operario: " + record.value());

                    
                     // Marcos, aquí debes parsear el record.value() para sacar la ID de la estación,
                      // buscarla en tu SQLite y ver si está en estado "AVAILABLE" (VERDE).
                     
                    
                    boolean estacionDisponible = true; //  Marcos cambia esto por tu consulta SQLite

                    if (estacionDisponible) {
                        System.out.println("[CENTRAL-KAFKA] Estación libre. Enviando autorización de riego...");
                        
                        producer.send(new ProducerRecord<>("ordenes-riego", record.value()));
                    } else {
                        System.out.println("[CENTRAL-KAFKA] Riego denegado: Estación ocupada, con fuga o fuera de servicio.");
                        
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[CENTRAL-KAFKA] Error crítico en el puente: " + e.getMessage());
        }
    }
}