import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * WM_Central: nucleo del sistema WaterManagement.
 *
 * Uso: java WM_Central <puerto_sockets> <ip_broker:puerto> [puerto_http] [ruta_bd]
 *
 *  - Servidor de sockets concurrente (un HiloServidor por monitor).
 *  - Consumidor Kafka de peticiones (FO / WS) y eventos de las WS (inicio, telemetria, fin).
 *  - Productor Kafka de ordenes a las WS, notificaciones a los FO y estado de la red (latido de Central).
 *  - Panel web (HTTP) con el estado de todas las WS y las ordenes de Central.
 *  - Persistencia en SQLite.
 */
public class WM_Central {
    // Topics Kafka
    static final String T_PETICIONES = "peticiones-riego";   // FO / WS  -> Central
    static final String T_ORDENES = "ordenes-ws";              // Central  -> WS
    static final String T_EVENTOS = "eventos-ws";              // WS       -> Central
    static final String T_NOTIF = "notificaciones-fo";         // Central  -> FO (key = id operario)
    static final String T_RED = "estado-red";                  // Central  -> todos (latido + estado de las WS)

    static final long PETICION_CADUCADA_MS = 30_000;
    static final long PENDIENTE_TIMEOUT_MS = 10_000;
    /** Un AUTH para una WS cuyo monitor envio latido hace menos de esto se considera un monitor duplicado. */
    static final long MONITOR_DUPLICADO_MS = 3_000;

    static final Object LOCK = new Object();
    static final Map<String, EstacionWS> estaciones = new TreeMap<>();
    static GestorBD bd;
    static KafkaProducer<String, String> productor;
    static volatile boolean kafkaOk = false;
    static volatile boolean panelSucio = true;
    static final Deque<String> registro = new ArrayDeque<>();
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        if (args.length < 2) {
            System.out.println("Uso: java WM_Central <puerto_sockets> <ip_broker:puerto> [puerto_http] [ruta_bd]");
            System.exit(1);
        }
        int puertoSockets = Integer.parseInt(args[0]);
        String broker = args[1];
        int puertoHttp = args.length >= 3 ? Integer.parseInt(args[2]) : 8000;
        String rutaBD = args.length >= 4 ? args[3] : "riego_sd.db";

        System.out.println("==============================================================");
        System.out.println("  WM_CENTRAL  -  WaterManagement");
        System.out.println("  Sockets monitores: " + puertoSockets + " | Kafka: " + broker
                + " | Panel web: " + puertoHttp + " | BD: " + rutaBD);
        System.out.println("==============================================================");

        // 1. Base de datos: cargar WS registradas (todas DESCONECTADAS hasta que conecten)
        bd = new GestorBD(rutaBD);
        for (String[] f : bd.cargarEstaciones()) {
            EstacionWS e = new EstacionWS(f[0], f[1]);
            e.bloqueada = "1".equals(f[2]);
            e.litrosTotales = Double.parseDouble(f[3]);
            estaciones.put(e.id, e);
        }
        log("[SQLITE] " + estaciones.size() + " WS registradas cargadas de la BD (estado inicial: DESCONECTADA)");
        imprimirPanel();

        // 2. Kafka
        crearTopics(broker);
        productor = new KafkaProducer<>(KafkaUtil.propsProductor(broker));
        hilo("kafka-consumidor", () -> bucleConsumidor(broker));
        hilo("latido-central", WM_Central::bucleLatido);

        // 3. Panel web
        ServidorWeb.arrancar(puertoHttp);

        // 4. Servidor de sockets concurrente para los monitores
        try (ServerSocket servidor = new ServerSocket(puertoSockets)) {
            servidor.setReuseAddress(true);
            log("[SOCKET] Escuchando monitores en el puerto " + puertoSockets);
            while (true) {
                Socket s = servidor.accept();
                new HiloServidor(s).start();
            }
        }
    }

    // ------------------------------------------------------------------ socket / monitores

    static void registrarEstacion(String id, String ubicacion) {
        ubicacion = limpiar(ubicacion);
        synchronized (LOCK) {
            EstacionWS e = estaciones.get(id);
            if (e == null) {
                e = new EstacionWS(id, ubicacion);
                estaciones.put(id, e);
                log("[REGISTRO] Nueva WS dada de alta: " + id + " (" + ubicacion + ")");
            } else if (!e.ubicacion.equals(ubicacion)) {
                log("[REGISTRO] " + id + " actualiza su ubicacion: " + ubicacion);
                e.ubicacion = ubicacion;
            }
            bd.registrarEstacion(id, ubicacion);
            panelSucio = true;
        }
    }

    /** Autentica la WS. Devuelve "OK#ACTIVA|BLOQUEADA" o "KO#motivo". */
    static String autenticar(String id, HiloServidor hilo, String origen) {
        synchronized (LOCK) {
            EstacionWS e = estaciones.get(id);
            if (e == null) {
                log("[AUTH] DENEGADA: " + id + " no esta registrada en WM_Central (desde " + origen + ")");
                return "KO#WS " + id + " no registrada en WM_Central";
            }
            if (e.monitor != null && e.monitor != hilo) {
                // Si el monitor actual sigue enviando latidos, el nuevo es un DUPLICADO (mismo ID en otra
                // ventana/PC). Antes se sustituia siempre y los dos se echaban mutuamente cada 3-4 s.
                if (System.currentTimeMillis() - e.ultimoLatido < MONITOR_DUPLICADO_MS) {
                    log("[AUTH] DENEGADA: " + id + " ya tiene un monitor activo; se rechaza el duplicado desde " + origen);
                    return "KO#" + id + " ya tiene un monitor conectado (cierra el otro o usa otro ID)";
                }
                log("[AUTH] " + id + " reconecta: se sustituye la conexion anterior del monitor");
                HiloServidor viejo = e.monitor;
                e.monitor = null;
                viejo.cerrar();
            }
            e.monitor = hilo;
            e.ultimoLatido = System.currentTimeMillis();
            e.salud = ""; // desconocida hasta el primer HEALTH
            actualizar(e);
            log("[AUTH] " + id + " (" + e.ubicacion + ") autenticada correctamente desde " + origen);
            return "OK#" + (e.bloqueada ? "BLOQUEADA" : "ACTIVA");
        }
    }

    /** Latido del monitor con la salud del engine. Devuelve "OK#ACTIVA|BLOQUEADA". */
    static String latido(String id, HiloServidor hilo, String salud) {
        synchronized (LOCK) {
            EstacionWS e = estaciones.get(id);
            if (e == null || e.monitor != hilo) return "KO#Sesion no valida";
            e.ultimoLatido = System.currentTimeMillis();
            String anterior = e.salud;
            e.salud = salud;
            if (!anterior.isEmpty() && !salud.equals(anterior)) {
                switch (salud) {
                    case EstacionWS.SALUD_FUGA:
                        log("[ALERTA] " + id + ": FUGA detectada por el monitor"); break;
                    case EstacionWS.SALUD_ENGINE_KO:
                        log("[ALERTA] " + id + ": el engine no responde (AVERIADA)"); break;
                    default:
                        if (EstacionWS.SALUD_FUGA.equals(anterior) || EstacionWS.SALUD_ENGINE_KO.equals(anterior))
                            log("[MONITOR] " + id + ": averia resuelta, engine OK");
                }
                // Averia durante un riego -> avisar al operario
                if ((EstacionWS.SALUD_ENGINE_KO.equals(salud)) && e.tieneRiego()) {
                    notificar(e.operario, e.idRiego, "ERROR", "AVERIA en " + id
                            + " durante su riego (engine caido). Recibira el resumen cuando la WS se recupere.");
                }
            }
            actualizar(e);
            return "OK#" + (e.bloqueada ? "BLOQUEADA" : "ACTIVA");
        }
    }

    static void monitorDesconectado(String id, HiloServidor hilo) {
        synchronized (LOCK) {
            EstacionWS e = estaciones.get(id);
            if (e == null || e.monitor != hilo) return; // ya habia sido sustituido por otra conexion
            e.monitor = null;
            log("[MONITOR] " + id + " DESCONECTADA (sin mensajes de vida del monitor)");
            if (e.tieneRiego()) {
                notificar(e.operario, e.idRiego, "ERROR", id + " se ha desconectado de WM_Central durante su riego.");
            }
            actualizar(e);
        }
    }

    // ------------------------------------------------------------------ Kafka

    private static void crearTopics(String broker) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        p.put("request.timeout.ms", "8000");
        p.put("default.api.timeout.ms", "10000");
        try (AdminClient admin = AdminClient.create(p)) {
            for (String t : List.of(T_PETICIONES, T_ORDENES, T_EVENTOS, T_NOTIF, T_RED)) {
                try {
                    admin.createTopics(List.of(new NewTopic(t, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
                } catch (ExecutionException ex) {
                    if (!(ex.getCause() instanceof TopicExistsException)) throw ex;
                }
            }
            kafkaOk = true;
            log("[KAFKA] Conectado a " + broker + ". Topics listos.");
        } catch (Exception e) {
            log("[KAFKA] AVISO: no se pudo contactar con el broker " + broker + " (" + e.getMessage()
                    + "). Se seguira reintentando.");
        }
    }

    private static void bucleConsumidor(String broker) {
        Properties p = KafkaUtil.propsConsumidor(broker, "wm-central", "earliest", "wm-central-1");
        while (true) {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                c.subscribe(List.of(T_PETICIONES, T_EVENTOS));
                while (true) {
                    ConsumerRecords<String, String> recs = c.poll(java.time.Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : recs) {
                        try {
                            if (T_PETICIONES.equals(r.topic())) procesarPeticion(r.value(), r.timestamp());
                            else procesarEvento(r.value(), r.timestamp());
                        } catch (Exception ex) {
                            log("[KAFKA] Mensaje ignorado (" + ex.getMessage() + "): " + r.value());
                        }
                    }
                }
            } catch (Exception e) {
                log("[KAFKA] Error en el consumidor: " + e.getMessage() + ". Reintentando en 3 s");
                dormir(3000);
            }
        }
    }

    /** PETICION#idPeticion#operario#ws#segundos   |   PARAR#idPeticion#operario#ws */
    static void procesarPeticion(String msg, long ts) {
        String[] c = msg.split("#", -1);
        String tipo = c[0];
        if ("LATIDO".equals(tipo)) {           // LATIDO#idOperario (cada 2 s desde cada FO)
            if (System.currentTimeMillis() - ts < 10_000) latidoOperario(c[1].trim());
            return;
        }
        String idPet = c[1];
        String op = c[2];
        String ws = c[3];

        if (System.currentTimeMillis() - ts > PETICION_CADUCADA_MS) {
            log("[PETICION] " + idPet + " de " + op + " descartada: caducada (WM_Central no estaba disponible)");
            notificar(op, idPet, "DENEGADA", "Peticion caducada: WM_Central no estaba disponible cuando se envio");
            return;
        }

        if ("PARAR".equals(tipo)) {
            synchronized (LOCK) {
                EstacionWS e = estaciones.get(ws);
                if (e != null && e.tieneRiego() && (e.operario.equals(op) || "CENTRAL".equals(op))) {
                    log("[PETICION] " + op + " solicita parar el riego de " + ws);
                    enviarOrden(ws, "PARAR#" + ws + "#PARADA_POR_" + op);
                    notificar(op, e.idRiego, "INFO", "Orden de parada enviada a " + ws);
                } else {
                    notificar(op, idPet, "INFO", "No tiene ningun riego activo en " + ws);
                }
            }
            return;
        }

        int seg;
        try { seg = Integer.parseInt(c[4].trim()); } catch (Exception ex) { seg = -1; }
        log("[PETICION] " + op + " solicita riego en " + ws + " durante " + durTxt(seg) + " (id " + idPet + ")");
        notificar(op, idPet, "INFO", "Peticion recibida en WM_Central. Validando disponibilidad de " + ws + "...");
        autorizarRiego(idPet, op, ws, seg);
    }

    /** Validacion comun para peticiones de FO, de la propia WS o de la Central. */
    static boolean autorizarRiego(String idPet, String op, String ws, int seg) {
        String motivo = null;
        boolean esFO = !op.startsWith("MANUAL-") && !"CENTRAL".equals(op);
        if (esFO && !bd.existeOperario(op)) motivo = "Operario " + op + " no registrado en WM_Central";
        else if (seg < 0 || seg > 3600) motivo = "Duracion no valida (1-3600 s, o 0 = sin limite)";

        synchronized (LOCK) {
            EstacionWS e = estaciones.get(ws);
            if (motivo == null && e == null) motivo = "La WS " + ws + " no existe";
            else if (motivo == null && !EstacionWS.AVAILABLE.equals(e.estado))
                motivo = ws + " no disponible (estado: " + e.estado + ")";

            if (motivo != null) {
                log("[PETICION] DENEGADA " + idPet + " (" + op + " -> " + ws + "): " + motivo);
                notificar(op, idPet, "DENEGADA", "Riego DENEGADO: " + motivo);
                return false;
            }
            e.limpiarRiego();
            e.idRiego = idPet;
            e.operario = op;
            e.objetivo = seg;
            e.pendienteDesde = System.currentTimeMillis();
            actualizar(e);
            enviarOrden(ws, "INICIAR#" + ws + "#" + seg + "#" + op + "#" + idPet);
            log("[PETICION] AUTORIZADA " + idPet + ": " + ws + " regara " + durTxt(seg) + " para " + op);
            notificar(op, idPet, "AUTORIZADA", "Riego AUTORIZADO en " + ws + " (" + durTxt(seg) + "). Orden enviada a la WS.");
            return true;
        }
    }

    /**
     * Eventos de las WS:
     *  INICIO#ws#op#idRiego#objetivo#caudal
     *  TEL#ws#op#idRiego#segundos#objetivo#caudal#volumen
     *  FIN#ws#op#idRiego#segundos#litros#motivo
     *  RECHAZO#ws#op#idRiego#motivo
     */
    static void procesarEvento(String msg, long ts) {
        String[] c = msg.split("#", -1);
        String tipo = c[0], ws = c[1], op = c[2], id = c[3];
        boolean reciente = System.currentTimeMillis() - ts < 10_000;

        synchronized (LOCK) {
            EstacionWS e = estaciones.get(ws);
            if (e == null) return;
            switch (tipo) {
                case "INICIO":
                    // INICIO viejo (p.ej. releido tras reiniciar la Central) o de un riego ya cerrado: ignorar,
                    // si no la WS se quedaba "regando" un riego que ya no existe
                    if (!reciente || finalizados.contains(id)) return;
                    e.idRiego = id;
                    e.operario = op;
                    e.objetivo = Integer.parseInt(c[4]);
                    e.caudal = Double.parseDouble(c[5]);
                    e.segundos = 0;
                    e.volumen = 0;
                    e.pendienteDesde = 0;
                    e.ultimaActividad = System.currentTimeMillis();
                    log("[RIEGO] " + ws + ": electrovalvula ABIERTA (" + durTxt(e.objetivo) + ", op " + op + ")");
                    notificar(op, id, "INICIO", ws + ": electrovalvula abierta, regando " + durTxt(e.objetivo));
                    actualizar(e);
                    break;
                case "TEL":
                    if (!reciente || finalizados.contains(id)) return; // vieja o de un riego ya terminado
                    if (!e.tieneRiego() || e.idRiego.equals(id)) {
                        e.ultimaActividad = System.currentTimeMillis();
                        e.idRiego = id;
                        e.operario = op;
                        e.segundos = Integer.parseInt(c[4]);
                        e.objetivo = Integer.parseInt(c[5]);
                        e.caudal = Double.parseDouble(c[6]);
                        e.volumen = Double.parseDouble(c[7]);
                        e.pendienteDesde = 0;
                        notificar(op, id, "TEL", String.format(Locale.ROOT, "%s regando | %.1f L/min | %.2f L | %d/%s s",
                                ws, e.caudal, e.volumen, e.segundos, e.objetivo == 0 ? "sin limite" : String.valueOf(e.objetivo)));
                        panelSucio = true;
                    }
                    break;
                case "FIN": {
                    int seg = Integer.parseInt(c[4]);
                    double litros = Double.parseDouble(c[5]);
                    String motivo = c[6];
                    boolean nuevo = bd.guardarRiego(id, ws, op, seg, litros, motivo);
                    finalizados.add(id);
                    if (finalizados.size() > 500) finalizados.remove(finalizados.iterator().next());
                    if (e.idRiego.equals(id)) e.limpiarRiego();
                    if (nuevo) {
                        e.litrosTotales += litros;
                        log(String.format(Locale.ROOT, "[RIEGO] %s FINALIZADO (%s) | %d s | %.2f L | op %s -> guardado en SQLite",
                                ws, motivo, seg, litros, op));
                        notificar(op, id, "FIN", String.format(Locale.ROOT,
                                "RESUMEN riego en %s | Motivo: %s | Duracion: %d s | Volumen total: %.2f L",
                                ws, motivo, seg, litros));
                    }
                    actualizar(e);
                    break;
                }
                case "RECHAZO":
                    if (e.idRiego.equals(id)) e.limpiarRiego();
                    log("[RIEGO] " + ws + " rechaza la orden " + id + ": " + c[4]);
                    notificar(op, id, "DENEGADA", ws + " ha rechazado el riego: " + c[4]);
                    actualizar(e);
                    break;
                default:
                    break;
            }
        }
    }

    /** Riegos ya finalizados (para ignorar telemetria que llegue tarde). Protegido por LOCK. */
    static final Set<String> finalizados = new LinkedHashSet<>();

    static void enviarOrden(String ws, String orden) {
        enviar(T_ORDENES, ws, orden);
    }

    /** NOTIF#operario#idPeticion#TIPO#texto  (solo a operarios reales) */
    static void notificar(String operario, String idPet, String tipo, String texto) {
        if (operario == null || operario.isEmpty() || "CENTRAL".equals(operario) || operario.startsWith("MANUAL-")) return;
        enviar(T_NOTIF, operario, "NOTIF#" + operario + "#" + idPet + "#" + tipo + "#" + texto.replace('#', ' '));
    }

    /** Envios en un hilo aparte: si Kafka cae, send() puede bloquear y no queremos frenar a la Central. */
    static final ThreadPoolExecutor envios = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(500), new ThreadPoolExecutor.DiscardOldestPolicy());

    static void enviar(String topic, String key, String valor) {
        envios.execute(() -> {
            try {
                productor.send(new ProducerRecord<>(topic, key, valor), (md, ex) -> estadoKafka(ex));
            } catch (Exception ex) {
                estadoKafka(ex);
            }
        });
    }

    private static void estadoKafka(Exception ex) {
        if (ex != null && kafkaOk) {
            kafkaOk = false;
            log("[KAFKA] ERROR: broker inaccesible (" + ex.getMessage() + ")");
            panelSucio = true;
        } else if (ex == null && !kafkaOk) {
            kafkaOk = true;
            log("[KAFKA] Conexion con el broker OK");
            panelSucio = true;
        }
    }

    // ------------------------------------------------------------------ ordenes de Central (panel web)

    /** accion = REGAR | BLOQUEAR | ACTIVAR ; ws = id o ALL */
    static String ordenCentral(String accion, String ws, int segundos) {
        List<String> destino = new ArrayList<>();
        synchronized (LOCK) {
            if ("ALL".equalsIgnoreCase(ws)) destino.addAll(estaciones.keySet());
            else if (estaciones.containsKey(ws)) destino.add(ws);
            else return "La WS " + ws + " no existe";
        }
        StringBuilder res = new StringBuilder();
        for (String id : destino) {
            synchronized (LOCK) {
                EstacionWS e = estaciones.get(id);
                switch (accion.toUpperCase()) {
                    case "BLOQUEAR":
                        e.bloqueada = true;
                        bd.setBloqueada(id, true);
                        log("[CENTRAL] Orden BLOQUEAR -> " + id + " (Out of Service)");
                        if (e.tieneRiego()) {
                            enviarOrden(id, "PARAR#" + id + "#BLOQUEO_CENTRAL");
                            notificar(e.operario, e.idRiego, "INFO", id + " bloqueada por WM_Central: el riego se detiene");
                        }
                        actualizar(e);
                        res.append(id).append(": bloqueada. ");
                        break;
                    case "ACTIVAR":
                        e.bloqueada = false;
                        bd.setBloqueada(id, false);
                        log("[CENTRAL] Orden ACTIVAR -> " + id);
                        actualizar(e);
                        res.append(id).append(": activada. ");
                        break;
                    case "PARAR":
                        if (e.tieneRiego()) {
                            log("[CENTRAL] Orden PARAR -> " + id);
                            enviarOrden(id, "PARAR#" + id + "#PARADA_POR_CENTRAL");
                            notificar(e.operario, e.idRiego, "INFO", "WM_Central ha parado el riego de " + id);
                            res.append(id).append(": orden de parada enviada. ");
                        } else {
                            res.append(id).append(": no tiene riego en curso. ");
                        }
                        break;
                    case "REGAR":
                        String idPet = "CEN-" + id + "-" + System.currentTimeMillis();
                        log("[CENTRAL] Orden REGAR -> " + id + " durante " + durTxt(segundos));
                        boolean ok = autorizarRiego(idPet, "CENTRAL", id, segundos);
                        res.append(id).append(ok ? ": riego iniciado. " : ": no disponible (" + e.estado + "). ");
                        break;
                    default:
                        return "Accion desconocida: " + accion;
                }
            }
        }
        return res.toString().trim();
    }

    // ------------------------------------------------------------------ operarios conectados

    static final long FO_TIMEOUT_MS = 6000;
    /** id operario -> ultimo latido recibido. Protegido por LOCK. */
    static final Map<String, Long> operarios = new TreeMap<>();
    static final Set<String> operariosConectados = new HashSet<>();

    static void latidoOperario(String id) {
        synchronized (LOCK) {
            operarios.put(id, System.currentTimeMillis());
            if (operariosConectados.add(id)) {
                log("[FO] Operario " + id + " CONECTADO" + (bd.existeOperario(id) ? "" : " (NO registrado en la BD)"));
                panelSucio = true;
            }
        }
    }

    /** Detecta operarios que han dejado de enviar latidos. Llamar con LOCK. */
    private static void revisarOperarios(long ahora) {
        for (Iterator<String> it = operariosConectados.iterator(); it.hasNext(); ) {
            String id = it.next();
            if (ahora - operarios.getOrDefault(id, 0L) > FO_TIMEOUT_MS) {
                it.remove();
                log("[FO] Operario " + id + " DESCONECTADO (sin latidos)");
                panelSucio = true;
            }
        }
    }

    /** Riego actual de un operario ("" si no tiene). Llamar con LOCK. */
    static String riegoDe(String op) {
        for (EstacionWS e : estaciones.values()) {
            if (e.tieneRiego() && e.operario.equals(op)) return e.id;
        }
        return "";
    }

    // ------------------------------------------------------------------ latido + panel

    private static void bucleLatido() {
        while (true) {
            dormir(1000);
            StringBuilder sb = new StringBuilder("RED#").append(System.currentTimeMillis()).append("#");
            synchronized (LOCK) {
                long ahora = System.currentTimeMillis();
                revisarOperarios(ahora);
                for (EstacionWS e : estaciones.values()) {
                    // Orden autorizada pero la WS nunca confirmo el inicio
                    if (e.pendienteDesde > 0 && ahora - e.pendienteDesde > PENDIENTE_TIMEOUT_MS
                            && !EstacionWS.SALUD_OK_REGANDO.equals(e.salud)) {
                        log("[RIEGO] " + e.id + " no confirmo el inicio del riego " + e.idRiego + ". Se cancela.");
                        notificar(e.operario, e.idRiego, "ERROR", e.id + " no ha respondido a la orden de riego");
                        e.limpiarRiego();
                        actualizar(e);
                    }
                    // Riego que la Central cree activo pero del que la WS ya no informa (p.ej. FIN perdido)
                    if (e.tieneRiego() && e.pendienteDesde == 0 && EstacionWS.SALUD_OK_IDLE.equals(e.salud)
                            && ahora - e.ultimaActividad > 5000) {
                        log("[RIEGO] " + e.id + ": la WS ya no esta regando. Se libera.");
                        e.limpiarRiego();
                        actualizar(e);
                    }
                    sb.append(e.id).append(',').append(e.ubicacion).append(',').append(e.estado).append(';');
                }
            }
            enviar(T_RED, "central", sb.toString());
            if (panelSucio) {
                panelSucio = false;
                imprimirPanel();
            }
        }
    }

    /** Recalcula el estado de la WS y lo guarda en BD si ha cambiado. Llamar con LOCK. */
    static void actualizar(EstacionWS e) {
        String nuevo = e.calcularEstado();
        if (!nuevo.equals(e.estado)) {
            e.estado = nuevo;
            bd.actualizarEstado(e.id, nuevo);
        }
        panelSucio = true;
    }

    static void imprimirPanel() {
        StringBuilder sb = new StringBuilder();
        String linea = "+--------+----------------------+----------------+----------+--------------------------------------------------+";
        sb.append('\n').append(linea).append('\n');
        sb.append(String.format("| %-6s | %-20s | %-14s | %8s | %-48s |%n", "ID", "UBICACION", "ESTADO", "TOTAL L", "DETALLE"));
        sb.append(linea).append('\n');
        synchronized (LOCK) {
            for (EstacionWS e : estaciones.values()) {
                sb.append(String.format(Locale.ROOT, "| %-6s | %-20.20s | %-14s | %8.1f | %-48.48s |%n",
                        e.id, e.ubicacion, e.estado, e.litrosTotales + e.volumen, e.detalle()));
            }
            sb.append(linea).append('\n').append("  Operarios conectados (").append(operariosConectados.size()).append("): ");
            if (operariosConectados.isEmpty()) sb.append("ninguno");
            for (String op : new TreeSet<>(operariosConectados)) {
                String r = riegoDe(op);
                sb.append(op).append(r.isEmpty() ? "" : " [regando " + r + "]").append("  ");
            }
            sb.append('\n');
        }
        sb.append(linea).append("  Kafka: ").append(kafkaOk ? "OK" : "SIN CONEXION");
        System.out.println(sb);
    }

    static void log(String texto) {
        String l = LocalTime.now().format(HORA) + " " + texto;
        System.out.println(l);
        synchronized (registro) {
            registro.addFirst(l);
            while (registro.size() > 150) registro.removeLast();
        }
    }

    /** Duracion para mostrar: 0 segundos = riego sin limite (hasta que alguien lo pare). */
    static String durTxt(int seg) {
        return seg == 0 ? "tiempo ilimitado" : seg + " s";
    }

    static String limpiar(String s) {
        return s.replaceAll("[#;,|]", " ").trim();
    }

    static void hilo(String nombre, Runnable r) {
        Thread t = new Thread(r, nombre);
        t.setDaemon(true);
        t.start();
    }

    static void dormir(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}
