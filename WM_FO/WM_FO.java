import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * WM_FO: aplicacion del operario de campo (Field Operator).
 *
 * Uso: java WM_FO <ip_broker:puerto> <id_operario> [fichero_peticiones]
 *
 *  - Solicita riegos a WM_Central por Kafka y recibe SOLO las notificaciones dirigidas a el
 *    (topic notificaciones-fo, clave = id del operario).
 *  - Ve todas las WS y su estado gracias al latido de Central (topic estado-red). Si deja de
 *    recibirlo durante 6 s muestra "Imposible conectar con WM_Central" y no envia peticiones.
 *  - Usa un grupo de consumidores fijo por operario: si el FO se cae durante un riego, al volver
 *    recibe los mensajes que se perdio (incluido el resumen final).
 *  - Modo fichero: envia cada peticion, espera a que concluya (exito o fracaso), espera 4 s y sigue.
 */
public class WM_FO {
    static final String RST = "\u001B[0m", ROJO = "\u001B[31m", VERDE = "\u001B[32m",
            AMARILLO = "\u001B[33m", CIAN = "\u001B[36m", GRIS = "\u001B[90m", NEGRITA = "\u001B[1m";
    static final String T_PETICIONES = "peticiones-riego", T_NOTIF = "notificaciones-fo", T_RED = "estado-red";

    static String idOperario;
    static KafkaProducer<String, String> productor;
    static final long inicio = System.currentTimeMillis();
    static volatile long ultimoLatido = 0;
    static volatile boolean centralOk = false;
    static volatile String red = "";                      // ultimo estado de la red recibido
    static final Object ESPERA = new Object();
    static volatile String peticionEsperada = null;
    static volatile String resultadoEsperado = null;
    static int contador = 0;

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        if (args.length < 2) {
            System.out.println("Uso: java WM_FO <ip_broker:puerto> <id_operario> [fichero_peticiones]");
            System.exit(1);
        }
        String broker = args[0];
        idOperario = args[1].trim();
        String fichero = args.length >= 3 ? args[2] : null;

        System.out.println(NEGRITA + CIAN + "==============================================");
        System.out.println("  WM_FO  -  Operario " + idOperario);
        System.out.println("  Broker: " + broker);
        System.out.println("==============================================" + RST);

        productor = new KafkaProducer<>(KafkaUtil.propsProductor(broker));
        hilo("kafka", () -> bucleConsumidor(broker));
        hilo("vigilante", WM_FO::bucleVigilante);
        // Latido del operario: asi WM_Central sabe que FO estan conectados y lo muestra en su panel
        hilo("latido", () -> {
            while (true) {
                try { productor.send(new ProducerRecord<>(T_PETICIONES, idOperario, "LATIDO#" + idOperario)); }
                catch (Exception ignored) { /* Kafka caido: lo detecta el vigilante */ }
                dormir(2000);
            }
        });

        log("Conectando con WM_Central...");
        for (int i = 0; i < 20 && !centralOk; i++) Thread.sleep(500);
        if (!centralOk) log(ROJO + NEGRITA + "Imposible conectar con WM_Central. Se seguira intentando.");

        if (fichero != null) ejecutarFichero(fichero);
        menu();
    }

    // ------------------------------------------------------------------ recepcion

    private static void bucleConsumidor(String broker) {
        // Grupo fijo por operario -> Kafka recuerda por donde se quedo leyendo (recuperacion tras caida)
        Properties p = KafkaUtil.propsConsumidor(broker, "wm-fo-" + idOperario, "latest", "fo-" + idOperario);
        while (true) {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                c.subscribe(List.of(T_NOTIF, T_RED));
                while (true) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                        if (T_RED.equals(r.topic())) {
                            if (System.currentTimeMillis() - r.timestamp() < 10_000) {
                                red = r.value();
                                ultimoLatido = System.currentTimeMillis();
                            }
                        } else if (idOperario.equals(r.key())) {
                            mostrarNotificacion(r.value(), r.timestamp() < inicio - 2000);
                        }
                    }
                }
            } catch (Exception e) {
                log(ROJO + "Error con el broker Kafka: " + e.getMessage() + ". Reintentando...");
                dormir(3000);
            }
        }
    }

    /** NOTIF#operario#idPeticion#TIPO#texto */
    private static void mostrarNotificacion(String msg, boolean atrasada) {
        String[] c = msg.split("#", 5);
        if (c.length < 5) return;
        String idPet = c[2], tipo = c[3], texto = c[4];
        String pre = atrasada ? AMARILLO + "[recibido tras reconectar] " + RST : "";
        String color;
        switch (tipo) {
            case "AUTORIZADA": case "INICIO": color = VERDE + NEGRITA; break;
            case "TEL": color = VERDE; texto = "  ~ " + texto; break;
            case "DENEGADA": case "ERROR": color = ROJO + NEGRITA; break;
            case "FIN": color = CIAN + NEGRITA; break;
            default: color = "";
        }
        log(pre + color + texto);

        if (idPet.equals(peticionEsperada) && (tipo.equals("DENEGADA") || tipo.equals("FIN") || tipo.equals("ERROR"))) {
            synchronized (ESPERA) {
                resultadoEsperado = tipo;
                ESPERA.notifyAll();
            }
        }
    }

    private static void bucleVigilante() {
        while (true) {
            dormir(1000);
            boolean ok = System.currentTimeMillis() - ultimoLatido < 6000;
            if (ok != centralOk) {
                centralOk = ok;
                if (ok) log(VERDE + NEGRITA + "Conectado con WM_Central");
                else log(ROJO + NEGRITA + "Imposible conectar con WM_Central (sin noticias de la Central / Kafka)");
            }
        }
    }

    // ------------------------------------------------------------------ envio de peticiones

    /** Devuelve el id de la peticion o null si no se pudo enviar. */
    private static String solicitarRiego(String ws, int segundos) {
        if (!centralOk) {
            log(ROJO + "Imposible conectar con WM_Central: peticion NO enviada.");
            return null;
        }
        String idPet = idOperario + "-" + System.currentTimeMillis() + "-" + (++contador);
        String msg = "PETICION#" + idPet + "#" + idOperario + "#" + ws + "#" + segundos;
        try {
            productor.send(new ProducerRecord<>(T_PETICIONES, idOperario, msg)).get(8, TimeUnit.SECONDS);
            log(CIAN + "Peticion enviada: regar " + ws + (segundos == 0 ? " SIN LIMITE (para con P " + ws + ")" : " durante " + segundos + " s")
                    + " (id " + idPet + ")");
            return idPet;
        } catch (Exception e) {
            log(ROJO + "Imposible conectar con WM_Central / Kafka: " + e.getMessage());
            return null;
        }
    }

    private static void pararRiego(String ws) {
        if (!centralOk) { log(ROJO + "Imposible conectar con WM_Central."); return; }
        String msg = "PARAR#" + idOperario + "-" + System.currentTimeMillis() + "#" + idOperario + "#" + ws;
        try {
            productor.send(new ProducerRecord<>(T_PETICIONES, idOperario, msg)).get(8, TimeUnit.SECONDS);
            log(CIAN + "Solicitud de parada enviada para " + ws);
        } catch (Exception e) {
            log(ROJO + "Imposible conectar con WM_Central / Kafka: " + e.getMessage());
        }
    }

    /** Lineas del fichero: WS-01#10  (estacion#segundos). Las lineas que empiezan por // se ignoran. */
    private static void ejecutarFichero(String ruta) {
        List<String> lineas;
        try {
            lineas = Files.readAllLines(new File(ruta).toPath(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log(ROJO + "No se puede leer el fichero " + ruta + ": " + e.getMessage());
            return;
        }
        log(NEGRITA + "Ejecutando " + lineas.size() + " lineas del fichero " + ruta);
        int n = 0;
        for (String l : lineas) {
            l = l.trim();
            if (l.isEmpty() || l.startsWith("//")) continue;
            String[] c = l.split("#");
            if (c.length < 2) { log(AMARILLO + "Linea ignorada (formato WS#segundos): " + l); continue; }
            int seg;
            try { seg = parseSegundos(c[1]); } catch (NumberFormatException e) { log(AMARILLO + "Linea ignorada: " + l); continue; }
            n++;
            log(NEGRITA + "--- Fichero: servicio " + n + " -> " + c[0].trim() + " (" + (seg == 0 ? "sin limite" : seg + " s") + ") ---");

            String idPet = solicitarRiego(c[0].trim(), seg);
            if (idPet != null) {
                // riego sin limite: se espera hasta que alguien lo pare
                String res = esperarConclusion(idPet, seg == 0 ? Long.MAX_VALUE / 4 : (seg + 60) * 1000L);
                log(NEGRITA + "Servicio " + n + " concluido: " + (res == null ? "sin respuesta (timeout)" : res));
            } else {
                log(NEGRITA + "Servicio " + n + " concluido: FALLIDO (no se pudo enviar)");
            }
            log("Esperando 4 segundos antes del siguiente servicio...");
            dormir(4000);
        }
        log(NEGRITA + "Fichero completado.");
    }

    private static String esperarConclusion(String idPet, long maxMs) {
        synchronized (ESPERA) {
            peticionEsperada = idPet;
            resultadoEsperado = null;
            long limite = System.currentTimeMillis() + maxMs;
            while (resultadoEsperado == null && System.currentTimeMillis() < limite) {
                try { ESPERA.wait(500); } catch (InterruptedException e) { break; }
            }
            peticionEsperada = null;
            return resultadoEsperado;
        }
    }

    // ------------------------------------------------------------------ menu

    private static void menu() {
        imprimirMenu();
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String linea = sc.nextLine().trim();
            if (linea.isEmpty()) continue;
            String[] in = linea.split("\\s+");
            String cmd = in[0].toUpperCase();
            try {
                if (linea.contains("#")) {                       // atajo: WS-01#10
                    String[] c = linea.split("#");
                    solicitarRiego(c[0].trim().toUpperCase(), parseSegundos(c[1]));
                } else if (cmd.equals("R")) {
                    String ws = in.length > 1 ? in[1] : preguntar(sc, "Estacion (ej. WS-01): ");
                    String seg = in.length > 2 ? in[2] : preguntar(sc, "Segundos de riego (INF = sin limite): ");
                    if (ws != null && seg != null) solicitarRiego(ws.toUpperCase(), parseSegundos(seg));
                } else if (cmd.equals("L")) {
                    listarEstaciones();
                } else if (cmd.equals("P")) {
                    String ws = in.length > 1 ? in[1] : preguntar(sc, "Estacion a parar: ");
                    if (ws != null) pararRiego(ws.toUpperCase());
                } else if (cmd.equals("F")) {
                    String f = in.length > 1 ? in[1] : preguntar(sc, "Fichero (ej. peticiones.txt): ");
                    if (f != null) ejecutarFichero(f);
                } else if (cmd.equals("S")) {
                    log("Saliendo...");
                    productor.close(Duration.ofSeconds(2));
                    System.exit(0);
                } else {
                    imprimirMenu();
                }
            } catch (NumberFormatException e) {
                log(AMARILLO + "Numero de segundos no valido");
            } catch (ArrayIndexOutOfBoundsException e) {
                log(AMARILLO + "Formato: WS-01#10");
            }
        }
        while (true) dormir(60_000); // sin teclado (contenedor sin -it)
    }

    /** "10" -> 10 ; "INF" / "INFINITO" / "I" -> 0 (0 = riego sin limite). Lanza NumberFormatException si no vale. */
    private static int parseSegundos(String txt) {
        String t = txt.trim().toUpperCase();
        if (t.equals("INF") || t.equals("INFINITO") || t.equals("I")) return 0;
        int n = Integer.parseInt(t);
        if (n < 0) throw new NumberFormatException(t);
        return n;
    }

    private static String preguntar(Scanner sc, String txt) {
        System.out.print(txt);
        return sc.hasNextLine() ? sc.nextLine().trim() : null;
    }

    private static void listarEstaciones() {
        if (!centralOk || red.isEmpty()) {
            log(ROJO + "Imposible conectar con WM_Central: no hay informacion de las estaciones.");
            return;
        }
        String[] c = red.split("#", 3);
        System.out.println(NEGRITA + "  ESTACION  UBICACION              ESTADO" + RST);
        for (String e : c[2].split(";")) {
            if (e.isBlank()) continue;
            String[] f = e.split(",");
            String col;
            switch (f[2]) {
                case "AVAILABLE": col = VERDE + NEGRITA; break;
                case "WATERING": col = VERDE; break;
                case "LEAK": col = ROJO; break;
                case "OUT_OF_SERVICE": col = AMARILLO; break;
                default: col = GRIS;
            }
            System.out.printf("  %-8s  %-22s %s%s%s%n", f[0], f[1], col,
                    "AVAILABLE".equals(f[2]) ? "AVAILABLE (disponible)" : f[2], RST);
        }
    }

    private static void imprimirMenu() {
        System.out.println(NEGRITA + "\n--- MENU OPERARIO " + idOperario + " ---" + RST);
        System.out.println(" WS-01#10        Pedir riego (estacion#segundos)");
        System.out.println(" WS-01#INF       Riego SIN LIMITE (se para con P WS-01)");
        System.out.println(" R [ws] [seg]    Pedir riego (seg = numero o INF)");
        System.out.println(" L               Ver estaciones y su estado");
        System.out.println(" P [ws]          Parar mi riego en una estacion");
        System.out.println(" F [fichero]     Ejecutar fichero de peticiones");
        System.out.println(" S               Salir");
    }

    // ------------------------------------------------------------------ utilidades

    static void hilo(String nombre, Runnable r) {
        Thread t = new Thread(r, nombre);
        t.setDaemon(true);
        t.start();
    }

    static void dormir(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    static synchronized void log(String msg) {
        System.out.println(GRIS + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + RST
                + " [FO " + idOperario + "] " + msg + RST);
    }
}
