import java.net.Socket;
import java.io.*;

public class HiloServidor extends Thread {
    private Socket skCliente;

    public HiloServidor(Socket p_cliente) {
        this.skCliente = p_cliente;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(skCliente.getInputStream()));
             PrintWriter out = new PrintWriter(skCliente.getOutputStream(), true)) {

            String trama;
            while ((trama = in.readLine()) != null) {
                String limpia = trama;
                int stx = limpia.indexOf((char) 2);
                int etx = limpia.indexOf((char) 3);
                if (stx != -1 && etx != -1 && etx > stx) {
                    limpia = limpia.substring(stx + 1, etx);
                }

                String idEstacion = "WS-01";
                String estado = "AVAILABLE";
                if (limpia.contains("KO")) {
                    estado = "FAULT";
                } else if (limpia.contains("REGANDO")) {
                    estado = "BUSY";
                }

                GestorBD.actualizarEstadoEstacion(idEstacion, estado);
                System.out.println("[WM_CENTRAL] Telemetría: " + limpia + " -> SQLite [" + idEstacion + " = " + estado + "]");

                out.println("ACK");
            }
        } catch (Exception e) {
            System.out.println("[WM_CENTRAL] Monitor desconectado: " + e.getMessage());
        }
    }
}