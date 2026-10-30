import java.net.*;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

public class ServidorConcurrente {
    // Historial compartido en memoria para sincronizar el frontend local
    private static final List<String> eventosGlobales = Collections.synchronizedList(new ArrayList<>());

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

        // Hilo Consumidor en modo Broadcast (group.id único por cada ordenador)
        new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", broker);
            // UUID único garantiza que TODOS los ordenadores reciban TODOS los mensajes de Kafka
            props.put("group.id", "central-sync-group-" + UUID.randomUUID().toString());
            props.put("auto.offset.reset", "latest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                // Escuchamos tanto los resúmenes finales como las órdenes en tiempo real
                consumer.subscribe(Arrays.asList("resumenes_riego", "peticiones-riego"));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        if ("resumenes_riego".equals(record.topic())) {
                            System.out.println("[KAFKA-AWS] Recibido resumen en la red: " + record.value()); 
        
                             GestorBD.registrarResumen(record.value());
                            eventosGlobales.add("RESUMEN|" + record.value());
                            } else if ("peticiones-riego".equals(record.topic())) {
                            // AÑADE ESTA LÍNEA para que se vea en todas las terminales
                            System.out.println("[KAFKA-AWS] Recibida orden desde la red: " + record.value());
        
        eventosGlobales.add("ORDEN|" + record.value());
    }
}
                }
            } catch (Exception e) {
                System.err.println("[CENTRAL-DB] Error en consumidor de sincronización: " + e.getMessage());
            }
        }).start();

        // Servidor HTTP para comunicarse con el index.html (Puerto 8082)
        new Thread(() -> {
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress(8082), 0);
                
                // Endpoint 1: Inyectar órdenes en Kafka
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
                                
                                byte[] response = "Orden procesada en Kafka".getBytes(StandardCharsets.UTF_8);
                                exchange.sendResponseHeaders(200, response.length);
                                OutputStream os = exchange.getResponseBody();
                                os.write(response);
                                os.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });

                // Endpoint 2: Devolver los eventos de Kafka al frontend para sincronizar pantallas
                server.createContext("/eventos", new HttpHandler() {
                    @Override
                    public void handle(HttpExchange exchange) {
                        try {
                            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
                            
                            StringBuilder sb = new StringBuilder();
                            synchronized (eventosGlobales) {
                                for (int i = 0; i < eventosGlobales.size(); i++) {
                                    sb.append(i).append("|").append(eventosGlobales.get(i)).append("\n");
                                }
                            }
                            
                            byte[] response = sb.toString().getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(200, response.length);
                            OutputStream os = exchange.getResponseBody();
                            os.write(response);
                            os.close();
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