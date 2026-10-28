import org.apache.kafka.clients.producer.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class WM_FO {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java WM_FO <ip_broker:puerto> [ruta_fichero.txt]");
            return;
        }

        String broker = args[0];
        String rutaFichero = (args.length >= 2) ? args[1] : "peticiones.txt";

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("[WM_FO] Iniciando lectura automatizada desde fichero: " + rutaFichero);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader br = new BufferedReader(new FileReader(rutaFichero))) {

            String linea;
            while ((linea = br.readLine()) != null) {
                linea = linea.trim();
                if (linea.isEmpty() || linea.startsWith("#")) continue;

                System.out.println("[WM_FO] Solicitando activación: " + linea);
                producer.send(new ProducerRecord<>("peticiones-riego", linea));
                producer.flush();

                System.out.println("[WM_FO] Esperando 4 segundos antes de la siguiente petición...");
                Thread.sleep(4000);
            }
            System.out.println("[WM_FO] Fin del archivo de peticiones.");
        } catch (Exception e) {
            System.err.println("[WM_FO] Error procesando fichero: " + e.getMessage());
        }
    }
}