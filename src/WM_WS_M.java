import java.io.*;
import java.net.*;

public class WM_WS_M {
    private static String idEstacion = "WS-01";
    private static String ubicacion = "ZonaNorte";
    private static String estadoAnterior = "Desconectada";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_M <IP_Central:Puerto> <Puerto_Escucha_Motor> [ID_WS] [Ubicacion]");
            return;
        }

        String[] centralParams = args[0].split(":");
        String centralIp = centralParams[0];
        int centralPort = Integer.parseInt(centralParams[1]);
        int puertoEscucha = Integer.parseInt(args[1]);
        if (args.length >= 3) idEstacion = args[2];
        if (args.length >= 4) ubicacion = args[3];

        try (Socket socketCentral = new Socket(centralIp, centralPort);
             DataInputStream inCentral = new DataInputStream(socketCentral.getInputStream());
             DataOutputStream outCentral = new DataOutputStream(socketCentral.getOutputStream());
             ServerSocket serverSocket = new ServerSocket(puertoEscucha)) {

            System.out.println("[MONITOR " + idEstacion + "] Enlazado a WM_Central " + centralIp + ":" + centralPort);

            enviarTrama(outCentral, "REGISTER#" + idEstacion + "#" + ubicacion);
            System.out.println("[MONITOR " + idEstacion + "] Autenticación Central: " + inCentral.readUTF());

            //si el Engine cae y se reinicia, el monitor vuelve a aceptarlo
            while (true) {
                System.out.println("[MONITOR " + idEstacion + "] Escuchando puerto " + puertoEscucha + " para conexión de WM_WS_E...");
                try (Socket socketMotor = serverSocket.accept();
                     BufferedReader inMotor = new BufferedReader(new InputStreamReader(socketMotor.getInputStream()));
                     PrintWriter outMotor = new PrintWriter(socketMotor.getOutputStream(), true)) {

                    socketMotor.setSoTimeout(2500); //detecta colapso del motor en 2.5 max
                    System.out.println("[MONITOR " + idEstacion + "] Engine conectado. Iniciando supervisión (1s).");

                    while (true) {
                        outMotor.println("STATUS");
                        String respuestaMotor = inMotor.readLine();

                        if (respuestaMotor == null) {
                            throw new IOException("Engine cerró el socket abruptamente (Crash)");
                        }

                        String estadoActual = "Disponible";
                        if (respuestaMotor.equals("KO")) {
                            estadoActual = "Fuga";
                        } else if (respuestaMotor.startsWith("REGANDO")) {
                            estadoActual = "Regando";
                        }

                        if (!estadoActual.equals(estadoAnterior)) {
                            enviarTrama(outCentral, "ALERT#" + idEstacion + "#" + estadoActual);
                            System.out.println("[MONITOR " + idEstacion + "] Notificado estado '" + estadoActual + "' -> Central: " + inCentral.readUTF());
                            estadoAnterior = estadoActual;
                        }

                        Thread.sleep(1000);
                    }
                } catch (Exception eMotor) {
                    System.err.println("[MONITOR " + idEstacion + "] ALERTA: Fallo/Caída en WM_WS_E (" + eMotor.getMessage() + ")");
                    if (!"Averiado".equals(estadoAnterior)) {
                        enviarTrama(outCentral, "ALERT#" + idEstacion + "#Averiado");
                        System.out.println("[MONITOR " + idEstacion + "] Estación marcada como AVERIADA en Central: " + inCentral.readUTF());
                        estadoAnterior = "Averiado";
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("\n[MONITOR " + idEstacion + "] Imposible conectar con WM_Central: " + e.getMessage());
        }
    }

    private static void enviarTrama(DataOutputStream out, String payload) throws IOException {
        String base = "<STX>" + payload + "<ETX>";
        byte lrc = 0;
        for (byte b : payload.getBytes()) {
            lrc ^= b;
        }
        out.writeUTF(base + lrc);
    }
}