import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Scanner;
import java.io.File;
// lo que hace este programa es simular un operador que envía peticiones de riego a estaciones de riego a través de Kafka. El operador se conecta a un broker de Kafka y envía mensajes JSON con la información del operador y la estación de riego. Se simula la lectura de un archivo con las estaciones de riego, y se envía una petición cada 4 segundos.
public class WM_FO {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_FO <ip_broker:puerto> <id_operador>");
            return;
        }
        
        String broker = args[0];
        String operatorId = args[1];
        
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Operador " + operatorId + " conectado a Kafka.");
            
            // Simulación de lectura de archivo (puedes ampliarlo para leer un TXT real)
            String[] peticiones = {"WS-01", "WS-02"}; 
            
            for (String estacion : peticiones) {
                String mensaje = "{\"operador\":\"" + operatorId + "\", \"estacion\":\"" + estacion + "\", \"accion\":\"iniciar_riego\"}";
                producer.send(new ProducerRecord<>("peticiones-riego", mensaje));
                System.out.println("Petición enviada para la estación: " + estacion);
                
                // Retraso estricto de 4 segundos exigido por la práctica
                Thread.sleep(4000); 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}