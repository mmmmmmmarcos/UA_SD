import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * WM_WS_M: Monitor de una Watering Station.
 *
 * Uso: java WM_WS_M <puerto_para_engine> <ip_central:puerto> <id_WS> [ubicacion]
 *   - Si se indica ubicacion, primero registra (da de alta) la WS en Central y luego se autentica.
 *   - Si no se indica, solo se autentica: Central la deniega si la WS no esta registrada.
 *
 * Hilo "central": conexion por sockets con WM_Central. Envia cada segundo HEALTH con la salud del engine.
 *                 Si Central cae, reintenta cada 3 s y avisa al engine (SIN_CENTRAL).
 * Hilo "engine":  servidor de sockets para el engine. Cada segundo le envia STATUS y espera OK/KO.
 *                 Si no responde (2.5 s) o responde KO -> se notifica averia/fuga a Central.
 */
public class WM_WS_M {
    static final String RST = "\u001B[0m", ROJO = "\u001B[31m", VERDE = "\u001B[32m",
            AMARILLO = "\u001B[33m", CIAN = "\u001B[36m", GRIS = "\u001B[90m", NEGRITA = "\u001B[1m";

    static String idWS;
    static String ubicacion;
    static volatile String saludEngine = "ENGINE_KO";  // OK_IDLE | OK_REGANDO | FUGA | ENGINE_KO
    static volatile String estadoCentral = "SIN_CENTRAL"; // ACTIVA | BLOQUEADA | SIN_CENTRAL

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java WM_WS_M <puerto_para_engine> <ip_central:puerto> <id_WS> [ubicacion]");
            System.exit(1);
        }
        int puertoEngine = Integer.parseInt(args[0]);
        String[] hp = args[1].split(":");
        String hostCentral = hp[0];
        int puertoCentral = Integer.parseInt(hp[1]);
        idWS = args[2].trim();
        ubicacion = args.length >= 4 ? String.join(" ", java.util.Arrays.copyOfRange(args, 3, args.length)).trim() : null;

        System.out.println(NEGRITA + CIAN + "==============================================");
        System.out.println("  WM_WS_M  -  MONITOR de la estacion " + idWS);
        System.out.println("  Central: " + hostCentral + ":" + puertoCentral + "  |  Puerto engine: " + puertoEngine);
        System.out.println("==============================================" + RST);

        Thread tEngine = new Thread(() -> servidorEngine(puertoEngine), "engine");
        tEngine.setDaemon(true);
        tEngine.start();

        bucleCentral(hostCentral, puertoCentral);
    }

    // ------------------------------------------------------------------ conexion con Central

    private static void bucleCentral(String host, int puerto) {
        boolean avisado = false;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, puerto), 3000);
                s.setSoTimeout(5000);
                Protocolo p = new Protocolo(s.getInputStream(), s.getOutputStream());

                if (ubicacion != null) {
                    String r = p.peticion("REGISTER#" + idWS + "#" + ubicacion);
                    log(r.startsWith("OK") ? VERDE + "Registro en WM_Central: " + idWS + " (" + ubicacion + ") OK"
                            : ROJO + "Registro rechazado: " + r);
                }
                String r = p.peticion("AUTH#" + idWS);
                if (!r.startsWith("OK")) {
                    log(ROJO + NEGRITA + "AUTENTICACION DENEGADA por WM_Central: " + campo(r, 1));
                    estadoCentral = "SIN_CENTRAL";
                    System.exit(2);
                }
                cambiarEstadoCentral(campo(r, 1));
                log(VERDE + NEGRITA + "Autenticado en WM_Central como " + idWS + RST);
                avisado = false;

                String saludEnviada = "";
                while (true) {
                    String salud = saludEngine;
                    r = p.peticion("HEALTH#" + idWS + "#" + salud);
                    if (!r.startsWith("OK")) throw new IOException("Central rechazo el latido: " + r);
                    cambiarEstadoCentral(campo(r, 1));
                    if (!salud.equals(saludEnviada)) {
                        log("Estado notificado a Central: " + describir(salud));
                        saludEnviada = salud;
                    }
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                estadoCentral = "SIN_CENTRAL";
                if (!avisado) {
                    log(ROJO + NEGRITA + "Imposible conectar con WM_Central (" + e.getMessage()
                            + "). Reintentando cada 3 s..." + RST);
                    avisado = true;
                }
            } catch (InterruptedException e) {
                return;
            }
            try { Thread.sleep(3000); } catch (InterruptedException e) { return; }
        }
    }

    private static void cambiarEstadoCentral(String nuevo) {
        if (!nuevo.equals(estadoCentral)) {
            estadoCentral = nuevo;
            if ("BLOQUEADA".equals(nuevo)) log(AMARILLO + NEGRITA + "WM_Central ha BLOQUEADO la estacion (Out of Service)");
            else if ("ACTIVA".equals(nuevo)) log(VERDE + "Estacion ACTIVA en WM_Central");
        }
    }

    // ------------------------------------------------------------------ servidor para el engine

    private static void servidorEngine(int puerto) {
        try (ServerSocket ss = new ServerSocket()) {
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(puerto));
            log("Esperando conexion del engine (WM_WS_E) en el puerto " + puerto + "...");
            while (true) {
                try (Socket s = ss.accept()) {
                    s.setSoTimeout(2500);
                    Protocolo p = new Protocolo(s.getInputStream(), s.getOutputStream());
                    log(VERDE + "Engine conectado desde " + s.getInetAddress().getHostAddress() + ". Comprobando su salud cada segundo.");
                    while (true) {
                        String r = p.peticion("STATUS#" + idWS + "#" + estadoCentral);
                        String nueva;
                        if (r.startsWith("OK#REGANDO")) nueva = "OK_REGANDO";
                        else if (r.startsWith("OK")) nueva = "OK_IDLE";
                        else nueva = "FUGA";
                        if (!nueva.equals(saludEngine)) {
                            if ("FUGA".equals(nueva)) log(ROJO + NEGRITA + "El engine responde KO: FUGA detectada -> aviso a Central");
                            else if ("FUGA".equals(saludEngine) || "ENGINE_KO".equals(saludEngine)) log(VERDE + "Engine OK");
                            saludEngine = nueva;
                        }
                        Thread.sleep(1000);
                    }
                } catch (IOException e) {
                    saludEngine = "ENGINE_KO";
                    log(ROJO + NEGRITA + "ALERTA: el engine no responde (" + e.getMessage()
                            + "). Estacion AVERIADA -> aviso a Central. Esperando reconexion...");
                }
            }
        } catch (IOException e) {
            log(ROJO + "No se puede abrir el puerto " + puerto + " para el engine: " + e.getMessage());
            System.exit(3);
        } catch (InterruptedException ignored) { }
    }

    // ------------------------------------------------------------------ utilidades

    private static String describir(String salud) {
        switch (salud) {
            case "OK_IDLE": return VERDE + "DISPONIBLE (engine OK)" + RST;
            case "OK_REGANDO": return VERDE + "REGANDO" + RST;
            case "FUGA": return ROJO + "FUGA" + RST;
            default: return ROJO + "AVERIADA (engine sin respuesta)" + RST;
        }
    }

    private static String campo(String s, int i) {
        String[] c = s.split("#", -1);
        return c.length > i ? c[i] : "";
    }

    static synchronized void log(String msg) {
        System.out.println(GRIS + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + RST
                + " [MONITOR " + idWS + "] " + msg + RST);
    }
}
