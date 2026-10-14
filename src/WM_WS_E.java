import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class WM_WS_E {
    private static volatile boolean averiaSimulada = false;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>");
            return;
        }

        String broker = args[0];

        // Hilo asíncrono para escuchar el teclado y simular la avería (fuga)
        new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            while (true) {
                String input = scanner.nextLine();
                if (input.equalsIgnoreCase("F")) {
                    averiaSimulada = true;
                    System.out.println("¡ATENCIÓN! Avería simulada (Fuga de agua detectada). Enviando KO al Monitor...");
                    // Aquí tendrías que enviar "KO" por socket al WM_WS_M que programará Marcos
                }
            }
        }).start();

        // Configuración Consumidor Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "estaciones-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("ordenes-riego"));
            System.out.println("Estación de riego iniciada. Pulsa 'F' y Enter para simular una fuga.");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (!averiaSimulada) {
                        System.out.println("Orden de riego recibida: " + record.value());
                        System.out.println("Válvula abierta. Suministrando agua...");
                    } else {
                        System.out.println("Orden ignorada. La estación tiene una fuga activa.");
                    }
                }
            }
        }
    }
}