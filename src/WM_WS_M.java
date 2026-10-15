import java.io.*;
import java.net.*;

public class WM_WS_M {
    private static String idEstacion = "WS-01";
    private static String ubicacion = "ZonaNorte";
    private static String estadoAnterior = "Disponible";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_M <IP_Central:Puerto> <Puerto_Escucha_Motor>");
            return;
        }

        String[] centralParams = args[0].split(":");
        String centralIp = centralParams[0];
        int centralPort = Integer.parseInt(centralParams[1]);
        int puertoEscucha = Integer.parseInt(args[1]);

        try (Socket socketCentral = new Socket(centralIp, centralPort);
             DataInputStream inCentral = new DataInputStream(socketCentral.getInputStream());
             DataOutputStream outCentral = new DataOutputStream(socketCentral.getOutputStream());
             ServerSocket serverSocket = new ServerSocket(puertoEscucha)) {

            System.out.println("[MONITOR] Enlazado a la Central " + centralIp + ":" + centralPort);
            
            enviarTrama(outCentral, "REGISTER#" + idEstacion + "#" + ubicacion);
            System.out.println("[MONITOR] Central responde: " + inCentral.readUTF());

            System.out.println("[MONITOR] Abriendo puerto " + puertoEscucha + ". Esperando arranque del motor...");
            
            try (Socket socketMotor = serverSocket.accept();
                 BufferedReader inMotor = new BufferedReader(new InputStreamReader(socketMotor.getInputStream()));
                 PrintWriter outMotor = new PrintWriter(socketMotor.getOutputStream(), true)) {
                 
                System.out.println("[MONITOR] Motor in situ conectado. Arrancando telemetria.");

                while (true) {
                    outMotor.println("STATUS");
                    String respuestaMotor = inMotor.readLine();
                    
                    if (respuestaMotor == null) {
                        System.out.println("[MONITOR] Conexion fisica con el motor perdida.");
                        break;
                    }

                    String estadoActual = "Disponible";
                    if (respuestaMotor.equals("KO")) {
                        estadoActual = "Fuga";
                    } else if (respuestaMotor.startsWith("REGANDO")) {
                        estadoActual = "Regando";
                    }

                    if (!estadoActual.equals(estadoAnterior)) {
                        enviarTrama(outCentral, "ALERT#" + idEstacion + "#" + estadoActual);
                        System.out.println("[MONITOR] Cambio de hardware. Central dice: " + inCentral.readUTF());
                        estadoAnterior = estadoActual;
                    }
                    
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            System.err.println("\n[MONITOR] Colapso de red: " + e.getMessage());
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