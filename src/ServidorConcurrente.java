import java.net.*;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

public class ServidorConcurrente {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java ServidorConcurrente <puerto_escucha> [broker_kafka]");
            System.exit(1);
        }

        int puerto = Integer.parseInt(args[0]);
        String broker = (args.length >= 2) ? args[1] : "kafka:9092";

        GestorBD.inicializarBD();

        Thread puente = new Thread(new PuenteKafkaCentral(broker));
        puente.start();

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

        new Thread(() -> {
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress(8082), 0);
                server.createContext("/regar", new HttpHandler() {
                    @Override
                    public void handle(HttpExchange exchange) {
                        try {
                            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                            String query = exchange.getRequestURI().getQuery();
                            
                            if (query != null && query.startsWith("orden=")) {
                                String ordenRaw = query.split("=")[1];
                                String orden = java.net.URLDecoder.decode(ordenRaw, "UTF-8");
                                
                                Properties prodProps = new Properties();
                                prodProps.put("bootstrap.servers", broker);
                                prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                                prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                                
                                try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
                                    producer.send(new ProducerRecord<>("peticiones-riego", orden));
                                    System.out.println("[WM_CENTRAL-HTTP] Orden inyectada desde Web: " + orden);
                                }
                                
                                String response = "Orden procesada en Kafka";
                                exchange.sendResponseHeaders(200, response.length());
                                OutputStream os = exchange.getResponseBody();
                                os.write(response.getBytes());
                                os.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                server.setExecutor(null);
                server.start();
                System.out.println("[WM_CENTRAL] Servidor HTTP escuchando frontend en puerto 8082...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

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