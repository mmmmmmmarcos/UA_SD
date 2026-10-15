import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WM_WS_E {
    // Memoria RAM compartida. Volatile fuerza la lectura directa.
    private static volatile boolean hayFuga = false;
    private static volatile boolean regando = false;
    private static volatile int segundosTranscurridos = 0;
    private static volatile int duracionObjetivo = 0;
    
    private static final double LITROS_MINUTO = 45.5; 
    private static final String ID_ESTACION = "WS-01"; 

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_E <Broker_IP:Puerto> <Monitor_IP:Puerto>");
            return;
        }

        String brokerUri = args[0];
        String[] monitorParams = args[1].split(":");
        String monitorIp = monitorParams[0];
        int monitorPort = Integer.parseInt(monitorParams[1]);

        // 1. EL CRONÓMETRO (Marcos)
        Thread timerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    if (regando) {
                        segundosTranscurridos++;
                        if (duracionObjetivo > 0 && segundosTranscurridos >= duracionObjetivo) {
                            detenerRiego(brokerUri);
                            System.out.println("\n[WS_ENGINE] Riego finalizado por temporizador.");
                            imprimirMenu();
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        timerThread.start();

        // 2. EL MENÚ TÁCTIL (Marcos)
        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            imprimirMenu();
            while (true) {
                String input = scanner.nextLine().trim().toUpperCase();
                
                if (input.equals("F")) {
                    hayFuga = !hayFuga;
                    System.out.println("[WS_ENGINE] Fuga " + (hayFuga ? "ACTIVADA" : "REPARADA"));
                } else if (input.equals("R")) {
                    if (!regando && !hayFuga) {
                        System.out.print("Segundos de riego: ");
                        try {
                            duracionObjetivo = Integer.parseInt(scanner.nextLine());
                            iniciarRiego();
                        } catch (Exception e) {
                            System.out.println("Formato inválido.");
                        }
                    } else {
                        System.out.println(hayFuga ? "Hay fuga. Imposible regar." : "El agua ya corre.");
                    }
                } else if (input.equals("P")) {
                    if (regando) {
                        detenerRiego(brokerUri);
                        System.out.println("[WS_ENGINE] Riego abortado manualmente.");
                    } else {
                        System.out.println("Válvula ya cerrada.");
                    }
                }
                imprimirMenu();
            }
        });
        inputThread.start();

        // 3. ANTENA KAFKA (Álvaro)
        Thread kafkaThread = new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", brokerUri);
            props.put("group.id", "estaciones-group-" + ID_ESTACION);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList("ordenes-riego"));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        if (hayFuga) {
                            System.out.println("\n[KAFKA] Orden ignorada. Fuga detectada en hardware.");
                            imprimirMenu();
                            continue;
                        }
                        
                        String orden = record.value();
                        System.out.println("\n[KAFKA] Orden remota de central: " + orden);
                        
                        if (orden.equalsIgnoreCase("STOP")) {
                            if (regando) detenerRiego(brokerUri);
                        } else {
                            try {
                                duracionObjetivo = Integer.parseInt(orden);
                                if (!regando) iniciarRiego();
                            } catch (NumberFormatException e) {
                                System.out.println("[KAFKA] Protocolo desconocido.");
                            }
                        }
                        imprimirMenu();
                    }
                }
            } catch (Exception e) {
                System.err.println("\n[KAFKA] Antena caída: " + e.getMessage());
            }
        });
        kafkaThread.start();

        // 4. EL PUENTE TCP (Marcos)
        try (Socket socket = new Socket(monitorIp, monitorPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            System.out.println("[WS_ENGINE] Conectado al monitor " + monitorIp + ":" + monitorPort);

            String peticionMonitor;
            while ((peticionMonitor = in.readLine()) != null) {
                if (hayFuga) {
                    out.println("KO"); 
                } else if (regando) {
                    out.println("REGANDO#" + LITROS_MINUTO);
                } else {
                    out.println("OK");
                }
            }
        } catch (IOException e) {
            System.err.println("\n[WS_ENGINE] Monitor desconectado: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void iniciarRiego() {
        regando = true;
        segundosTranscurridos = 0;
        System.out.println("[WS_ENGINE] Válvula ABIERTA.");
    }

    private static void detenerRiego(String brokerUri) {
        regando = false;
        System.out.println("[WS_ENGINE] Válvula CERRADA.");
        notificarFinRiego(brokerUri, ID_ESTACION, segundosTranscurridos, LITROS_MINUTO);
    }

    private static void imprimirMenu() {
        System.out.println("\n=== PANEL WS_ENGINE ===");
        System.out.println("[R] Abrir válvula (Manual)");
        System.out.println("[P] Parar riego (Manual)");
        System.out.println("[F] Provocar/Arreglar Fuga");
        System.out.print("> ");
    }

    private static void notificarFinRiego(String brokerUri, String idEstacion, int duracionSegundos, double litrosPorMinuto) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUri);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        double volumenTotal = (duracionSegundos / 60.0) * litrosPorMinuto;
        String resumen = idEstacion + "#" + duracionSegundos + "#" + String.format("%.2f", volumenTotal).replace(',', '.');

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("resumenes_riego", idEstacion, resumen));
            System.out.println("[KAFKA] Resumen disparado: " + resumen);
        } catch (Exception e) {
            System.err.println("[KAFKA] Cola inaccesible: " + e.getMessage());
        }
    }
}