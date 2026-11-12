import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * WM_WS_E: Engine de una Watering Station (caudalimetro + electrovalvula + detector de fugas).
 *
 * Uso: java WM_WS_E <ip_broker:puerto> <ip_monitor:puerto> [caudal_L_min] [carpeta_datos]
 *
 *  - El ID de la WS NO se pasa por parametro: lo recibe de su monitor (WM_WS_M), que es quien
 *    se autentica en WM_Central.
 *  - Recibe ordenes de Central por Kafka (topic ordenes-ws) y le envia cada segundo la telemetria
 *    del riego (topic eventos-ws).
 *  - Guarda el riego en curso en disco cada segundo: si el engine cae, al volver a arrancar
 *    envia el estado final del riego para que no se pierda nada.
 */
public class WM_WS_E {
    static final String RST = "\u001B[0m", ROJO = "\u001B[31m", VERDE = "\u001B[32m",
            AMARILLO = "\u001B[33m", CIAN = "\u001B[36m", GRIS = "\u001B[90m", NEGRITA = "\u001B[1m";
    static final String T_PETICIONES = "peticiones-riego", T_ORDENES = "ordenes-ws", T_EVENTOS = "eventos-ws";

    static volatile String idWS = null;
    static volatile boolean monitorOk = false;
    static volatile String estadoCentral = "SIN_CENTRAL";
    static volatile boolean fuga = false;

    // Riego en curso (protegido por LOCK). 'regando' es volatile para que el hilo del monitor pueda
    // leerlo SIN coger el LOCK: si no, cualquier retardo del temporizador (disco, consola) hacia que
    // el engine no contestase a tiempo al monitor y este lo daba por caido (falsa FUGA/AVERIA).
    static final Object LOCK = new Object();
    static volatile boolean regando = false;
    static int segundos, objetivo;
    static double volumen;
    static String operario = "", idRiego = "";

    static double caudal = 45.5;
    static volatile KafkaProducer<String, String> productor;   // null hasta que el broker sea accesible
    static long inicioRiego;   // ms en que se abrio la valvula (protegido por LOCK)
    static File ficheroRiego, ficheroPendientes;
    static final Object LOCK_PENDIENTES = new Object();
    /** Envios asincronos: si Kafka cae no bloqueamos el temporizador ni el monitor. */
    static final ThreadPoolExecutor envios = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(30), new ThreadPoolExecutor.DiscardOldestPolicy());

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_E <ip_broker:puerto> <ip_monitor:puerto> [caudal_L_min] [carpeta_datos]");
            System.exit(1);
        }
        String broker = args[0];
        String hostMonitor;
        int puertoMonitor;
        try {
            String[] hp = args[1].split(":");
            hostMonitor = hp[0];
            puertoMonitor = Integer.parseInt(hp[1].trim());
            if (args.length >= 3) caudal = Double.parseDouble(args[2].trim());
            if (caudal <= 0) throw new NumberFormatException("caudal <= 0");
        } catch (RuntimeException e) {
            System.out.println("Parametros no validos: el monitor debe ser ip:puerto y el caudal un numero positivo (L/min).");
            System.out.println("Uso: java WM_WS_E <ip_broker:puerto> <ip_monitor:puerto> [caudal_L_min] [carpeta_datos]");
            System.exit(1);
            return;
        }
        File dir = new File(args.length >= 4 ? args[3] : "datos");
        dir.mkdirs();
        ficheroRiego = new File(dir, "riego_en_curso.txt");
        ficheroPendientes = new File(dir, "resumenes_pendientes.txt");

        System.out.println(NEGRITA + CIAN + "==============================================");
        System.out.println("  WM_WS_E  -  ENGINE de estacion de riego");
        System.out.println("  Broker: " + broker + "  |  Monitor: " + hostMonitor + ":" + puertoMonitor
                + "  |  Caudal: " + caudal + " L/min");
        System.out.println("==============================================" + RST);

        // En otro hilo: si el broker no resuelve, el engine sigue contestando a su monitor mientras reintenta
        hilo("kafka-productor", () -> productor = KafkaUtil.crearProductor(broker, m -> log(ROJO + m)));
        recuperarRiegoInterrumpido();

        hilo("pendientes", () -> { while (true) { reenviarPendientes(); dormir(3000); } });
        hilo("temporizador", WM_WS_E::bucleTemporizador);
        hilo("monitor", () -> bucleMonitor(hostMonitor, puertoMonitor, broker));

        menu();
    }

    // ------------------------------------------------------------------ enlace con el monitor

    private static void bucleMonitor(String host, int puerto, String broker) {
        boolean avisado = false;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, puerto), 3000);
                s.setSoTimeout(5000);
                Protocolo p = new Protocolo(s.getInputStream(), s.getOutputStream());
                while (true) {
                    String[] c = p.recibir().split("#", -1);   // STATUS#idWS#estadoCentral
                    if (idWS == null) {
                        idWS = c[1];
                        log(VERDE + NEGRITA + "ID asignado por el monitor: " + idWS);
                        hilo("kafka-ordenes", () -> bucleOrdenes(broker));
                    }
                    if (!monitorOk) {
                        monitorOk = true;
                        avisado = false;
                        log(VERDE + "Conectado a su monitor " + host + ":" + puerto);
                    }
                    cambiarEstadoCentral(c.length > 2 ? c[2] : "SIN_CENTRAL");
                    // Sin LOCK a proposito (ver comentario de 'regando'): la respuesta debe salir al instante
                    p.enviar(fuga ? "KO#FUGA" : (regando ? "OK#REGANDO" : "OK#IDLE"));
                }
            } catch (IOException e) {
                if (monitorOk || !avisado) {
                    log(ROJO + NEGRITA + "ESTACION AVERIADA: sin conexion con su monitor (" + e.getMessage()
                            + "). No se admiten riegos. Reintentando...");
                    avisado = true;
                }
                monitorOk = false;
                detenerRiego("MONITOR_CAIDO");
            }
            dormir(2000);
        }
    }

    private static void cambiarEstadoCentral(String nuevo) {
        if (nuevo.equals(estadoCentral)) return;
        String anterior = estadoCentral;
        estadoCentral = nuevo;
        switch (nuevo) {
            case "SIN_CENTRAL":
                log(ROJO + NEGRITA + "Imposible conectar con WM_Central. El riego en curso (si lo hay) continuara "
                        + "hasta finalizar; no se admiten nuevos riegos.");
                break;
            case "BLOQUEADA":
                log(AMARILLO + NEGRITA + "WM_Central ha BLOQUEADO la estacion: OUT OF SERVICE");
                // En otro hilo: estamos en el hilo que contesta al monitor y no debe bloquearse
                hilo("parada-bloqueo", () -> detenerRiego("BLOQUEO_CENTRAL"));
                break;
            default:
                log(VERDE + ("SIN_CENTRAL".equals(anterior) ? "Conexion con WM_Central OK. " : "")
                        + "Estacion ACTIVA");
        }
    }

    // ------------------------------------------------------------------ ordenes desde Central (Kafka)

    private static void bucleOrdenes(String broker) {
        Properties p = KafkaUtil.propsConsumidor(broker, "ws-" + idWS + "-" + UUID.randomUUID(), "latest");
        String ultimoError = null;   // el mismo error solo se muestra una vez (si no, sale cada 3 s)
        while (true) {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                ultimoError = null;
                c.subscribe(List.of(T_ORDENES));
                while (true) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                        if (System.currentTimeMillis() - r.timestamp() > 15_000) continue; // orden caducada
                        String[] o = r.value().split("#", -1);
                        if (o.length < 2 || !(o[1].equals(idWS) || o[1].equalsIgnoreCase("ALL"))) continue;
                        try {
                            if ("INICIAR".equals(o[0])) ordenIniciar(Integer.parseInt(o[2]), o[3], o[4]);
                            else if ("PARAR".equals(o[0])) {
                                log(AMARILLO + "Orden de Central: PARAR (" + o[2] + ")");
                                detenerRiego(o[2]);
                            }
                        } catch (RuntimeException ex) {   // orden mal formada: se ignora, el consumidor sigue
                            log(AMARILLO + "Orden ignorada (formato no valido): " + r.value());
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

    private static void ordenIniciar(int seg, String op, String id) {
        String motivo = null;
        synchronized (LOCK) {
            if (!monitorOk) motivo = "Estacion averiada (sin monitor)";
            else if (fuga) motivo = "Fuga detectada";
            // No se comprueba aqui si la WS esta BLOQUEADA: quien decide es la Central, que ya lo valido antes
            // de enviar la orden. El estadoCentral local llega por el latido con 1-2 s de retraso y, justo
            // despues de un ACTIVAR, hacia rechazar riegos que la Central acababa de autorizar.
            else if (regando) motivo = "Ya esta regando";
            if (motivo == null) {
                regando = true;
                inicioRiego = System.currentTimeMillis();
                segundos = 0;
                volumen = 0;
                objetivo = seg;
                operario = op;
                idRiego = id;
                guardarRiego();
            }
        }
        if (motivo != null) {
            log(ROJO + "Orden de riego de " + op + " RECHAZADA: " + motivo);
            enviar(T_EVENTOS, "RECHAZO#" + idWS + "#" + op + "#" + id + "#" + motivo);
            return;
        }
        log(VERDE + NEGRITA + "Orden de Central: ELECTROVALVULA ABIERTA " + (seg == 0 ? "SIN LIMITE (hasta que se pare)" : "durante " + seg + " s") + " (solicitado por " + op + ")");
        enviar(T_EVENTOS, "INICIO#" + idWS + "#" + op + "#" + id + "#" + seg + "#" + fmt(caudal));
    }

    // ------------------------------------------------------------------ riego

    private static void bucleTemporizador() {
        while (true) {
            // Se comprueba cada 100 ms y cada segundo cuenta desde que se abrio la valvula: con un sleep(1000)
            // fijo el primer "segundo" podia durar casi 0 y un riego de N s duraba en realidad N-1.
            dormir(100);
            boolean fin = false;
            synchronized (LOCK) {
                if (!regando) continue;
                if (System.currentTimeMillis() - inicioRiego < (segundos + 1) * 1000L) continue;
                segundos++;
                volumen += caudal / 60.0;
                guardarRiego();
                enviar(T_EVENTOS, "TEL#" + idWS + "#" + operario + "#" + idRiego + "#" + segundos + "#" + objetivo
                        + "#" + fmt(caudal) + "#" + fmt(volumen));
                System.out.println(VERDE + "  ~ Regando " + idWS + "  " + segundos + "/" + (objetivo == 0 ? "∞" : objetivo) + " s | Caudal: "
                        + fmt(caudal) + " L/min | Acumulado: " + fmt(volumen) + " L | Op: " + operario + RST);
                if (objetivo > 0 && segundos >= objetivo) fin = true;   // objetivo 0 = riego sin limite
            }
            if (fin) detenerRiego("COMPLETADO");
        }
    }

    static void detenerRiego(String motivo) {
        String lineaFin;
        synchronized (LOCK) {
            if (!regando) return;
            regando = false;
            lineaFin = "FIN#" + idWS + "#" + operario + "#" + idRiego + "#" + segundos + "#" + fmt(volumen) + "#" + motivo;
            log(AMARILLO + NEGRITA + "ELECTROVALVULA CERRADA (" + motivo + "). Total: " + segundos + " s, "
                    + fmt(volumen) + " L");
            encolarResumen(lineaFin);
            ficheroRiego.delete();
        }
        hilo("envio-fin", WM_WS_E::reenviarPendientes);
    }

    /** Guarda el riego en curso para no perderlo si el engine cae. Llamar con LOCK. */
    private static void guardarRiego() {
        String linea = idWS + "#" + operario + "#" + idRiego + "#" + segundos + "#" + objetivo + "#" + fmt(volumen);
        try {
            Files.writeString(ficheroRiego.toPath(), linea, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log(ROJO + "No se pudo guardar el estado del riego: " + e.getMessage());
        }
    }

    /** Al arrancar: si habia un riego a medias (el engine cayo), se envia su estado final. */
    private static void recuperarRiegoInterrumpido() {
        if (!ficheroRiego.exists()) return;
        try {
            String[] c = Files.readString(ficheroRiego.toPath(), StandardCharsets.UTF_8).trim().split("#", -1);
            String fin = "FIN#" + c[0] + "#" + c[1] + "#" + c[2] + "#" + c[3] + "#" + c[5] + "#AVERIA_ENGINE";
            log(AMARILLO + NEGRITA + "Recuperado riego interrumpido por caida del engine: " + c[0] + ", " + c[3]
                    + " s, " + c[5] + " L (op " + c[1] + "). Se envia el estado final a Central.");
            synchronized (LOCK) { encolarResumen(fin); }
            ficheroRiego.delete();
        } catch (Exception e) {
            log(ROJO + "Fichero de riego corrupto, se descarta: " + e.getMessage());
            ficheroRiego.delete();
        }
    }

    private static void encolarResumen(String linea) {
        synchronized (LOCK_PENDIENTES) {
            try {
                Files.writeString(ficheroPendientes.toPath(), linea + "\n", StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                log(ROJO + "No se pudo guardar el resumen pendiente: " + e.getMessage());
            }
        }
    }

    /** Un solo hilo reenviando a la vez (el periodico y los "envio-fin" no se pisan). */
    static final Object LOCK_ENVIO = new Object();

    /**
     * Envia a Central los resumenes de riego pendientes. Solo se borran cuando Kafka confirma.
     * El fichero (LOCK_PENDIENTES) solo se bloquea para leerlo y reescribirlo, NUNCA mientras se espera
     * a Kafka (hasta 8 s): detenerRiego() lo necesita estando dentro del LOCK del riego.
     */
    private static void reenviarPendientes() {
        KafkaProducer<String, String> prod = productor;
        if (prod == null) return;   // broker aun no accesible: el hilo "pendientes" lo reintentara
        synchronized (LOCK_ENVIO) {
            List<String> lineas;
            synchronized (LOCK_PENDIENTES) {
                if (!ficheroPendientes.exists()) return;
                try {
                    lineas = Files.readAllLines(ficheroPendientes.toPath(), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    log(ROJO + "Error con el fichero de pendientes: " + e.getMessage());
                    return;
                }
            }
            int procesadas = 0;   // lineas del principio del fichero ya enviadas (o vacias)
            for (String l : lineas) {
                if (!l.isBlank()) {
                    try {
                        prod.send(new ProducerRecord<>(T_EVENTOS, l.split("#")[1], l)).get(8, TimeUnit.SECONDS);
                        log(CIAN + "Resumen final del riego enviado a WM_Central: " + l);
                    } catch (Exception e) {
                        log(ROJO + "No se pudo enviar el resumen (Kafka inaccesible). Se reintentara.");
                        break;
                    }
                }
                procesadas++;
            }
            if (procesadas == 0) return;
            // Mientras enviabamos solo se han podido AÑADIR lineas al final: quitamos las 'procesadas' primeras
            synchronized (LOCK_PENDIENTES) {
                try {
                    List<String> actuales = Files.readAllLines(ficheroPendientes.toPath(), StandardCharsets.UTF_8);
                    List<String> restantes = actuales.subList(Math.min(procesadas, actuales.size()), actuales.size());
                    if (restantes.isEmpty()) ficheroPendientes.delete();
                    else Files.write(ficheroPendientes.toPath(), restantes, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    log(ROJO + "Error con el fichero de pendientes: " + e.getMessage());
                }
            }
        }
    }

    // ------------------------------------------------------------------ menu de la estacion

    private static void menu() {
        imprimirMenu();
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String[] in = sc.nextLine().trim().toUpperCase().split("\\s+");
            switch (in[0]) {
                case "R": solicitarRiegoManual(in.length > 1 ? in[1] : null, sc); break;
                case "P":
                    boolean habia;
                    synchronized (LOCK) { habia = regando; }
                    if (habia) detenerRiego("PARADA_MANUAL_WS");
                    else log("La valvula ya esta cerrada.");
                    break;
                case "F":
                    fuga = !fuga;
                    if (fuga) {
                        log(ROJO + NEGRITA + "FUGA SIMULADA: el engine respondera KO al monitor");
                        detenerRiego("FUGA");
                    } else {
                        log(VERDE + NEGRITA + "Fuga REPARADA: el engine vuelve a responder OK");
                    }
                    break;
                case "E": imprimirEstado(); break;
                case "": break;
                default: imprimirMenu();
            }
        }
        // stdin cerrado (p.ej. contenedor sin -it): seguimos funcionando sin menu
        while (true) dormir(60_000);
    }

    private static void solicitarRiegoManual(String segTxt, Scanner sc) {
        if (idWS == null || !monitorOk) {
            log(ROJO + "Estacion AVERIADA (sin monitor): no se admiten riegos ni manuales.");
            return;
        }
        if ("SIN_CENTRAL".equals(estadoCentral)) { log(ROJO + "Imposible conectar con WM_Central: no se admiten riegos."); return; }
        if ("BLOQUEADA".equals(estadoCentral)) { log(AMARILLO + "Estacion Out of Service."); return; }
        if (fuga) { log(ROJO + "Hay una fuga: imposible regar."); return; }
        synchronized (LOCK) { if (regando) { log("Ya se esta regando."); return; } }
        if (segTxt == null) {
            System.out.print("Segundos de riego: ");
            if (!sc.hasNextLine()) return;
            segTxt = sc.nextLine().trim();
        }
        int seg;
        String t = segTxt.trim().toUpperCase();
        if (t.equals("INF") || t.equals("INFINITO") || t.equals("I")) seg = 0;   // 0 = sin limite
        else {
            try { seg = Integer.parseInt(t); } catch (NumberFormatException e) { log("Numero no valido (usa segundos o INF)."); return; }
            if (seg < 0) { log("Numero no valido."); return; }
        }
        String idPet = "MAN-" + idWS + "-" + System.currentTimeMillis();
        enviar(T_PETICIONES, "PETICION#" + idPet + "#MANUAL-" + idWS + "#" + idWS + "#" + seg);
        log(CIAN + "Peticion de riego manual (" + (seg == 0 ? "sin limite" : seg + " s") + ") enviada a WM_Central para su validacion...");
    }

    private static void imprimirMenu() {
        System.out.println(NEGRITA + "\n--- MENU ENGINE " + (idWS == null ? "" : idWS) + " ---" + RST);
        System.out.println(" R [seg]  Solicitar riego desde la estacion (R INF = sin limite, se para con P)");
        System.out.println(" P        Parar el riego en curso");
        System.out.println(" F        Simular FUGA / reparar (el engine responde KO al monitor)");
        System.out.println(" E        Ver estado");
        System.out.println("(escribe la letra y pulsa Enter)");
    }

    private static void imprimirEstado() {
        synchronized (LOCK) {
            System.out.println(NEGRITA + "Estado " + idWS + RST + ": monitor=" + (monitorOk ? "OK" : "CAIDO")
                    + " | central=" + estadoCentral + " | fuga=" + (fuga ? "SI" : "NO")
                    + " | " + (regando ? "REGANDO " + segundos + "/" + (objetivo == 0 ? "∞" : objetivo) + " s, " + fmt(volumen) + " L, op " + operario
                    : "valvula cerrada"));
        }
    }

    // ------------------------------------------------------------------ utilidades

    static void enviar(String topic, String valor) {
        String key = idWS == null ? "?" : idWS;
        envios.execute(() -> {
            KafkaProducer<String, String> p = productor;
            if (p == null) { log(ROJO + "Kafka inaccesible: mensaje no enviado"); return; }
            try {
                p.send(new ProducerRecord<>(topic, key, valor));
            } catch (Exception e) {
                log(ROJO + "Kafka inaccesible: " + e.getMessage());
            }
        });
    }

    static String fmt(double d) {
        return String.format(Locale.ROOT, "%.2f", d);
    }

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
                + " [ENGINE " + (idWS == null ? "?" : idWS) + "] " + msg + RST);
    }
}
