import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WM_WS_E {
    // Variables volátiles. La memoria RAM se comparte entre hilos sin bloqueos.
    private static volatile boolean hayFuga = false;
    private static volatile boolean regando = false;
    private static volatile int segundosTranscurridos = 0;
    private static volatile int duracionObjetivo = 0;
    
    private static final double LITROS_MINUTO = 45.5; // Caudal fijo.
    private static final String ID_ESTACION = "WS-01"; // TBD: Pasar por argumentos luego.

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_E <Broker_IP:Puerto> <Monitor_IP:Puerto>");
            return;
        }

        String brokerUri = args[0];
        String[] monitorParams = args[1].split(":");
        String monitorIp = monitorParams[0];
        int monitorPort = Integer.parseInt(monitorParams[1]);

        // EL CRONÓMETRO: Hilo fantasma que vigila el tiempo de riego
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

        // EL MENÚ TÁCTIL: Captura órdenes físicas del operario in situ
        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            imprimirMenu();
            while (true) {
                String input = scanner.nextLine().trim().toUpperCase();
                
                if (input.equals("F")) {
                    hayFuga = !hayFuga;
                    System.out.println("[WS_ENGINE] Fuga " + (hayFuga ? "ACTIVADA" : "REPARADA"));
                } else if (input.equals("R")) {
                    if (!regando) {
                        System.out.print("Segundos de riego: ");
                        try {
                            duracionObjetivo = Integer.parseInt(scanner.nextLine());
                            iniciarRiego();
                        } catch (Exception e) {
                            System.out.println("Formato inválido.");
                        }
                    } else {
                        System.out.println("El agua ya está corriendo.");
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

        // LA RED: Conexión TCP pura con tu monitor WM_WS_M
        try (Socket socket = new Socket(monitorIp, monitorPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            System.out.println("[WS_ENGINE] Conectado al monitor " + monitorIp + ":" + monitorPort);

            String peticionMonitor;
            while ((peticionMonitor = in.readLine()) != null) {
                // Tu monitor pregunta. Tú escupes el estado actual de la máquina.
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