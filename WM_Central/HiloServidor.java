import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Hilo que atiende a UN monitor (WM_WS_M) conectado por socket.
 * Mensajes que entiende (dentro de tramas <STX>DATA<ETX><LRC>):
 *   REGISTER#idWS#ubicacion   -> alta / actualizacion de la WS en SQLite
 *   AUTH#idWS                 -> autenticacion (se deniega si la WS no esta registrada)
 *   HEALTH#idWS#salud         -> latido cada segundo con la salud del engine
 * Si no llega ningun mensaje en 5 s se considera que el monitor ha caido -> DISCONNECTED.
 */
public class HiloServidor extends Thread {
    private static final int TIMEOUT_LATIDO_MS = 5000;

    private final Socket socket;
    private String idWS = null;

    public HiloServidor(Socket socket) {
        this.socket = socket;
        setDaemon(true);
        setName("monitor-" + socket.getRemoteSocketAddress());
    }

    public void cerrar() {
        try { socket.close(); } catch (IOException ignored) { }
    }

    @Override
    public void run() {
        String origen = socket.getInetAddress().getHostAddress();
        try {
            socket.setSoTimeout(TIMEOUT_LATIDO_MS);
            Protocolo p = new Protocolo(socket.getInputStream(), socket.getOutputStream());

            while (true) {
                String msg = p.recibir();
                String[] c = msg.split("#", -1);
                String op = c[0].toUpperCase();

                switch (op) {
                    case "REGISTER": {
                        if (c.length < 3 || c[1].isBlank() || c[2].isBlank()) {
                            p.enviar("KO#Formato REGISTER#idWS#ubicacion");
                            break;
                        }
                        WM_Central.registrarEstacion(c[1].trim(), c[2].trim());
                        p.enviar("OK#REGISTRADA");
                        break;
                    }
                    case "AUTH": {
                        if (c.length < 2) { p.enviar("KO#Formato AUTH#idWS"); break; }
                        String resp = WM_Central.autenticar(c[1].trim(), this, origen);
                        if (resp.startsWith("OK")) idWS = c[1].trim();
                        p.enviar(resp);
                        break;
                    }
                    case "HEALTH": {
                        if (idWS == null) { p.enviar("KO#Monitor no autenticado"); break; }
                        String salud = c.length >= 3 ? c[2].trim() : EstacionWS.SALUD_ENGINE_KO;
                        p.enviar(WM_Central.latido(idWS, this, salud));
                        break;
                    }
                    default:
                        p.enviar("KO#Operacion desconocida: " + op);
                }
            }
        } catch (SocketTimeoutException e) {
            if (idWS != null) WM_Central.log("[SOCKET] " + idWS + ": sin latidos del monitor en "
                    + (TIMEOUT_LATIDO_MS / 1000) + " s");
        } catch (IOException e) {
            if (idWS != null) WM_Central.log("[SOCKET] " + idWS + ": conexion con monitor perdida (" + e.getMessage() + ")");
        } finally {
            cerrar();
            if (idWS != null) WM_Central.monitorDesconectado(idWS, this);
        }
    }
}
