import java.net.Socket;
import java.io.*;

public class HiloServidor extends Thread {
    private Socket skCliente;
    private String idEstacionAsociada = "WS-01";

    public HiloServidor(Socket p_cliente) {
        this.skCliente = p_cliente;
    }

    @Override
    public void run() {
        try (DataInputStream in = new DataInputStream(skCliente.getInputStream());
             DataOutputStream out = new DataOutputStream(skCliente.getOutputStream())) {

            while (true) {
                String trama = in.readUTF(); // Emparejado con el writeUTF de WM_WS_M

                int stx = trama.indexOf("<STX>");
                int etx = trama.indexOf("<ETX>");

                if (stx != -1 && etx != -1 && etx > stx) {
                    String payload = trama.substring(stx + 5, etx);
                    String lrcRecibidoStr = trama.substring(etx + 5);

                    // Cálculo XOR byte a byte para verificar integridad (LRC)
                    byte lrcCalculado = 0;
                    for (byte b : payload.getBytes()) {
                        lrcCalculado ^= b;
                    }

                    if (!String.valueOf(lrcCalculado).equals(lrcRecibidoStr.trim())) {
                        System.err.println("[WM_CENTRAL] Trama corrupta (LRC inválido). Enviando NACK.");
                        out.writeUTF("NACK");
                        continue;
                    }

                    // Parseo de protocolo: REGISTER#WS-01#ZonaNorte o ALERT#WS-01#Estado
                    String[] partes = payload.split("#");
                    String operacion = partes[0];
                    if (partes.length >= 2) {
                        idEstacionAsociada = partes[1];
                    }

                    if ("REGISTER".equalsIgnoreCase(operacion)) {
                        String ubicacion = (partes.length >= 3) ? partes[2] : "Desconocida";
                        GestorBD.registrarEstacion(idEstacionAsociada, ubicacion, "AVAILABLE");
                        ServidorConcurrente.eventosGlobales.add("ESTADO_TCP|" + idEstacionAsociada + "#Disponible");
                        System.out.println("[WM_CENTRAL] Estación autenticada y registrada: " + idEstacionAsociada + " (" + ubicacion + ") [LRC OK]");
                        out.writeUTF("ACK#REGISTERED");
                    } else if ("ALERT".equalsIgnoreCase(operacion)) {
                        String estadoReportado = (partes.length >= 3) ? partes[2] : "Disponible";
                        String estadoBD = "AVAILABLE";

                        if (estadoReportado.equalsIgnoreCase("Fuga") || estadoReportado.equalsIgnoreCase("Averiado")) {
                            estadoBD = "FAULT";
                        } else if (estadoReportado.equalsIgnoreCase("Regando")) {
                            estadoBD = "BUSY";
                        }

                        GestorBD.actualizarEstadoEstacion(idEstacionAsociada, estadoBD);
                        ServidorConcurrente.eventosGlobales.add("ESTADO_TCP|" + idEstacionAsociada + "#" + estadoReportado);
                        System.out.println("[WM_CENTRAL] Telemetría [" + idEstacionAsociada + "]: " + estadoReportado + " -> SQLite [" + estadoBD + "] [LRC OK]");
                        out.writeUTF("ACK#" + estadoBD);
                    } else {
                        out.writeUTF("ACK");
                    }
                } else {
                    out.writeUTF("NACK");
                }
            }
        } catch (Exception e) {
            System.err.println("[WM_CENTRAL] Monitor de " + idEstacionAsociada + " desconectado abruptamente (" + e.getMessage() + ").");
            GestorBD.actualizarEstadoEstacion(idEstacionAsociada, "DISCONNECTED");
            ServidorConcurrente.eventosGlobales.add("ESTADO_TCP|" + idEstacionAsociada + "#Desconectada");
        }
    }
}