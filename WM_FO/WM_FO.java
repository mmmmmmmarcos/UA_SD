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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

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
    /** Identifica a ESTA ventana de WM_FO (para detectar el mismo operario abierto dos veces). */
    static final String TOKEN = UUID.randomUUID().toString().substring(0, 8);
    static volatile KafkaProducer<String, String> productor;   // null hasta que el broker sea accesible
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
        if (!idOperario.matches("[A-Za-z0-9_-]{1,32}")) {
            System.out.println("ID de operario no valido: '" + idOperario + "' (solo letras, numeros, '-' y '_', max. 32)");
            System.exit(1);
        }

        System.out.println(NEGRITA + CIAN + "==============================================");
        System.out.println("  WM_FO  -  Operario " + idOperario);
        System.out.println("  Broker: " + broker);
        System.out.println("==============================================" + RST);

        hilo("kafka-productor", () -> productor = KafkaUtil.crearProductor(broker, m -> log(ROJO + m)));
        hilo("estado-red", () -> bucleRed(broker));
        hilo("vigilante", WM_FO::bucleVigilante);
        // Latido del operario: asi WM_Central sabe que FO estan conectados y lo muestra en su panel.
        // Lleva el TOKEN de esta ventana para que la Central detecte un mismo ID abierto dos veces.
        hilo("latido", () -> {
            while (true) {
                KafkaProducer<String, String> p = productor;
                if (p != null) {
                    try { p.send(new ProducerRecord<>(T_PETICIONES, idOperario, "LATIDO#" + idOperario + "#" + TOKEN)); }
                    catch (Exception ignored) { /* Kafka caido: lo detecta el vigilante */ }
                }
                dormir(2000);
            }
        });

        log("Conectando con WM_Central...");
        comprobarDuplicado();
        // Solo despues de comprobar que no hay otra ventana con este ID nos unimos al grupo del operario
        hilo("kafka", () -> bucleConsumidor(broker));
        if (!centralOk) log(ROJO + NEGRITA + "Imposible conectar con WM_Central. Se seguira intentando.");

        if (fichero != null) ejecutarFichero(fichero);
        menu();
    }

    // ------------------------------------------------------------------ recepcion

    private static void bucleConsumidor(String broker) {
        // Grupo fijo por operario -> Kafka recuerda por donde se quedo leyendo (recuperacion tras caida)
        Properties p = KafkaUtil.propsConsumidor(broker, "wm-fo-" + idOperario, "latest", "fo-" + idOperario);
        String ultimoError = null;   // el mismo error solo se muestra una vez (si no, sale cada 3 s)
        while (true) {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                ultimoError = null;
                c.subscribe(List.of(T_NOTIF));
                while (true) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                        if (idOperario.equals(r.key())) {
                            mostrarNotificacion(r.value(), r.timestamp() < inicio - 2000);
                        }
                    }
                }
            } catch (Exception e) {
                if (!String.valueOf(e.getMessage()).equals(ultimoError)) {
                    ultimoError = String.valueOf(e.getMessage());
                    log(ROJO + "Error con el broker Kafka: " + ultimoError + ". Reintentando...");
                }
                dormir(3000);
            }
        }
    }

    /**
     * Latido de la Central (topic estado-red). Se lee SIN grupo de consumidores (assign): cada ventana
     * recibe todos los latidos aunque haya otra con el mismo ID, y asi puede detectar que es un duplicado.
     */
    private static void bucleRed(String broker) {
        Properties p = KafkaUtil.propsConsumidor(broker, "sin-grupo", "latest");
        p.remove("group.id");
        p.put("enable.auto.commit", "false");
        while (true) {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                List<TopicPartition> parts = new ArrayList<>();
                List<PartitionInfo> info = c.partitionsFor(T_RED);
                if (info != null) for (PartitionInfo pi : info) parts.add(new TopicPartition(T_RED, pi.partition()));
                if (parts.isEmpty()) throw new IllegalStateException("el topic " + T_RED + " aun no existe");
                c.assign(parts);
                c.seekToEnd(parts);
                while (true) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                        if (System.currentTimeMillis() - r.timestamp() < 10_000) {
                            red = r.value();
                            ultimoLatido = System.currentTimeMillis();
                        }
                    }
                }
            } catch (Exception e) {
                dormir(3000);   // el vigilante ya avisa de "Imposible conectar con WM_Central"
            }
        }
    }

    /**
     * Antes de unirse al grupo Kafka del operario, comprueba que no haya otra ventana abierta con el mismo ID:
     * dos consumidores con el mismo group.instance.id se expulsan mutuamente y uno se queda sin notificaciones.
     * La Central publica en estado-red el token de la ventana aceptada para cada operario.
     * Si el token es de otra ventana, se espera un poco por si es una sesion anterior que se cayo
     * (la Central la olvida a los 6 s sin latidos) y, si sigue ahi, se cierra este FO.
     */
    private static void comprobarDuplicado() {
        long t0 = System.currentTimeMillis();
        boolean avisado = false;
        while (true) {
            long pasado = System.currentTimeMillis() - t0;
            String t = tokenEnRed();
            if (TOKEN.equals(t)) return;                                  // la Central nos ha aceptado
            if (red.isEmpty() && pasado > 10_000) return;                 // sin Central: no se puede comprobar
            if (t != null && !t.isEmpty()) {
                if (!avisado) {
                    log(AMARILLO + "WM_Central ya tiene un " + idOperario + " conectado. Comprobando si es una sesion anterior que se cayo...");
                    avisado = true;
                }
                if (pasado > 14_000) {
                    log(ROJO + NEGRITA + "Ya hay otra ventana del operario " + idOperario
                            + " conectada a WM_Central. Cierra la otra o usa otro ID. Saliendo.");
                    System.exit(2);
                }
            }
            if (pasado > 20_000) return;   // Central no confirma (version antigua?): seguimos
            dormir(500);
        }
    }

    /** Token que la Central tiene aceptado para este operario (null si no aparece en el ultimo estado-red). */
    private static String tokenEnRed() {
        String[] c = red.split("#", -1);   // RED#ts#estaciones#operario=token;...
        if (c.length < 4) return null;
        for (String par : c[3].split(";")) {
            int i = par.indexOf('=');
            if (i > 0 && par.substring(0, i).equals(idOperario)) return par.substring(i + 1);
        }
        return null;
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
        if (!centralOk || productor == null) {
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
        if (!centralOk || productor == null) { log(ROJO + "Imposible conectar con WM_Central."); return; }
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

            String idPet = solicitarRiego(c[0].trim().toUpperCase(), seg);   // igual que en el menu
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
                    if (productor != null) productor.close(Duration.ofSeconds(2));
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
        String[] c = red.split("#", -1);   // RED#ts#ws,ubicacion,estado;...#operarios
        if (c.length < 3) { log(ROJO + "Estado de la red no valido."); return; }
        System.out.println(NEGRITA + "  ESTACION  UBICACION              ESTADO" + RST);
        for (String e : c[2].split(";")) {
            if (e.isBlank()) continue;
            String[] f = e.split(",");
            if (f.length < 3) continue;   // entrada mal formada (p.ej. un ID antiguo con ',' en la BD): se salta
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
