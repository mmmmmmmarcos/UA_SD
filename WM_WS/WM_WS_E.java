import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.Properties;
import java.util.Collections;
import java.util.UUID;
import java.time.Duration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WM_WS_E {
    private static volatile boolean hayFuga = false;
    private static volatile boolean fueraServicio = false;
    private static volatile boolean regando = false;
    private static volatile int segundosTranscurridos = 0;
    private static volatile int duracionObjetivo = 0;
    private static volatile String operarioActual = "MANUAL";

    private static final double LITROS_MINUTO = 45.5;
    private static String ID_ESTACION = "WS-01";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_E <Broker_IP:Puerto> <Monitor_IP:Puerto> [ID_WS]");
            return;
        }

        String brokerUri = args[0];
        String[] monitorParams = args[1].split(":");
        String monitorIp = monitorParams[0];
        int monitorPort = Integer.parseInt(monitorParams[1]);
        if (args.length >= 3) ID_ESTACION = args[2];

        // 1. EL CRONÓMETRO FÍSICO DEL ENGINE
        Thread timerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    if (regando) {
                        segundosTranscurridos++;
                        double vol = (segundosTranscurridos / 60.0) * LITROS_MINUTO;
                        System.out.println("[WS_ENGINE " + ID_ESTACION + "] Regando... (" + segundosTranscurridos + "/" + duracionObjetivo + "s) | Caudal: " + LITROS_MINUTO + " L/min | Acumulado: " + String.format("%.2f", vol) + " L | Op: " + operarioActual);
                        if (duracionObjetivo > 0 && segundosTranscurridos >= duracionObjetivo) {
                            System.out.println("\n[WS_ENGINE " + ID_ESTACION + "] Tiempo objetivo alcanzado. Cerrando electroválvula.");
                            detenerRiego(brokerUri);
                            imprimirMenu();
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        timerThread.start();

        // 2. MENÚ INTERACTIVO EN CONSOLA
        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            imprimirMenu();
            while (scanner.hasNextLine()) {
                String input = scanner.nextLine().trim().toUpperCase();

                if (input.equals("F")) {
                    hayFuga = !hayFuga;
                    System.out.println("[WS_ENGINE] Fuga " + (hayFuga ? "ACTIVADA (KO)" : "REPARADA (OK)"));
                    if (hayFuga && regando) {
                        System.out.println("[WS_ENGINE] Emergencia: Fuga durante riego. Abortando suministro inmediatamente.");
                        detenerRiego(brokerUri);
                    }
                } else if (input.equals("R")) {
                    if (!regando && !hayFuga && !fueraServicio) {
                        System.out.print("Segundos de riego: ");
                        try {
                            duracionObjetivo = Integer.parseInt(scanner.nextLine().trim());
                            operarioActual = "MANUAL-WS";
                            iniciarRiego();
                        } catch (Exception e) {
                            System.out.println("Formato inválido.");
                        }
                    } else {
                        System.out.println(hayFuga ? "Averiada/Fuga. Imposible regar." : (fueraServicio ? "Out of Service." : "El agua ya corre."));
                    }
                } else if (input.equals("P")) {
                    if (regando) {
                        detenerRiego(brokerUri);
                        System.out.println("[WS_ENGINE] Riego finalizado manualmente.");
                    } else {
                        System.out.println("Válvula ya cerrada.");
                    }
                }
                imprimirMenu();
            }
        });
        inputThread.start();

        // 3. ANTENA KAFKA (Escucha tanto ordenes-riego como peticiones-riego para máxima compatibilidad)
        Thread kafkaThread = new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", brokerUri);
            props.put("group.id", "estaciones-group-" + ID_ESTACION + "-" + UUID.randomUUID());
            props.put("auto.offset.reset", "latest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList("ordenes-riego"));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        String raw = record.value().trim();
                        String accion = raw;
                        String op = "FO-01";

                        // Soporta tanto "5" como "WS-01#5" o "WS-01#5#FO-01"
                        if (raw.contains("#")) {
                            String[] partes = raw.split("#");
                            if (!partes[0].equalsIgnoreCase(ID_ESTACION) && !partes[0].equalsIgnoreCase("ALL")) {
                                continue; // Es para otra estación
                            }
                            accion = partes[1];
                            if (partes.length >= 3) op = partes[2];
                        }

                        System.out.println("\n[KAFKA -> " + ID_ESTACION + "] Instrucción recibida: " + accion + " (Solicitante: " + op + ")");

                        if (accion.equalsIgnoreCase("FUGA")) {
                            hayFuga = !hayFuga;
                            System.out.println("[WS_ENGINE] Fuga " + (hayFuga ? "ACTIVADA" : "REPARADA"));
                            if (hayFuga && regando) detenerRiego(brokerUri);
                        } else if (accion.equalsIgnoreCase("BLOCK")) {
                            fueraServicio = true;
                            if (regando) detenerRiego(brokerUri);
                            System.out.println("[WS_ENGINE] Estación bloqueada por CENTRAL (Out of Service).");
                        } else if (accion.equalsIgnoreCase("ACTIVATE")) {
                            fueraServicio = false;
                            hayFuga = false;
                            System.out.println("[WS_ENGINE] Estación reactivada por CENTRAL (Available).");
                        } else if (accion.equalsIgnoreCase("STOP")) {
                            if (regando) detenerRiego(brokerUri);
                        } else if (!hayFuga && !fueraServicio) {
                            try {
                                duracionObjetivo = Integer.parseInt(accion);
                                operarioActual = op;
                                if (!regando) iniciarRiego();
                            } catch (NumberFormatException e) {
                                System.out.println("[KAFKA] Comando no numérico ignorado: " + accion);
                            }
                        } else {
                            System.out.println("[KAFKA] Orden rechazada: Estación en Fuga o Fuera de Servicio.");
                        }
                        imprimirMenu();
                    }
                }
            } catch (Exception e) {
                System.err.println("\n[KAFKA] Error de conexión con Broker: " + e.getMessage());
            }
        });
        kafkaThread.start();

        // 4. PUENTE TCP CON EL MONITOR (Con reconexión automática si el monitor se reinicia)
        while (true) {
            try (Socket socket = new Socket(monitorIp, monitorPort);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                System.out.println("[WS_ENGINE " + ID_ESTACION + "] Enlazado al monitor local " + monitorIp + ":" + monitorPort);

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
                System.err.println("\n[WS_ENGINE] Monitor caído o inalcanzable (" + e.getMessage() + "). Estación averiada sin supervisión.");
                if (regando) {
                    System.out.println("[WS_ENGINE] Deteniendo riego por pérdida de Monitor...");
                    detenerRiego(brokerUri);
                }
                try { Thread.sleep(3000); } catch (InterruptedException ie) { break; }
            }
        }
    }

    private static void iniciarRiego() {
        regando = true;
        segundosTranscurridos = 0;
        System.out.println("[WS_ENGINE " + ID_ESTACION + "] Electroválvula ABIERTA por " + duracionObjetivo + "s.");
    }

    private static synchronized void detenerRiego(String brokerUri) {
        if (!regando) return;
        regando = false;
        int duracionReal = segundosTranscurridos;
        segundosTranscurridos = 0;
        System.out.println("[WS_ENGINE " + ID_ESTACION + "] Electroválvula CERRADA.");
        notificarFinRiego(brokerUri, ID_ESTACION, duracionReal, LITROS_MINUTO, operarioActual);
    }

    private static void imprimirMenu() {
        System.out.println("\n=== PANEL WS_ENGINE (" + ID_ESTACION + ") ===");
        System.out.println("[R] Abrir válvula (Manual)");
        System.out.println("[P] Parar riego (Manual)");
        System.out.println("[F] Provocar/Arreglar Fuga (KO/OK)");
        System.out.print("> ");
    }

    private static void notificarFinRiego(String brokerUri, String idEstacion, int duracionSegundos, double litrosPorMinuto, String op) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUri);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        double volumenTotal = (duracionSegundos / 60.0) * litrosPorMinuto;
        String resumen = idEstacion + "#" + duracionSegundos + "#" + String.format("%.2f", volumenTotal).replace(',', '.') + "#" + op;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("resumenes_riego", idEstacion, resumen));
            producer.flush();
            System.out.println("[KAFKA] Resumen final enviado a Central y Operario: " + resumen);
        } catch (Exception e) {
            System.err.println("[KAFKA] Cola inaccesible al enviar resumen: " + e.getMessage());
        }
    }
}