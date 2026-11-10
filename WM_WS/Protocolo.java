import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Protocolo de sockets basado en tramas  <STX><DATA><ETX><LRC>
 *
 *  - STX = 0x02, ETX = 0x03
 *  - DATA = "OPERACION#campo1#campo2#..." codificado en UTF-8
 *  - LRC  = XOR byte a byte de DATA
 *
 * Cada trama recibida se confirma con un byte <ACK> (0x06) si el LRC es correcto
 * o <NACK> (0x15) si no lo es. Ante un NACK el emisor reintenta (max. 3 veces).
 *
 * (Este fichero es identico en WM_WS y WM_Central porque ambos hablan el mismo protocolo.)
 */
public final class Protocolo {
    public static final int STX = 0x02;
    public static final int ETX = 0x03;
    public static final int ACK = 0x06;
    public static final int NACK = 0x15;
    private static final int MAX_REINTENTOS = 3;

    private final InputStream in;
    private final OutputStream out;

    public Protocolo(InputStream in, OutputStream out) {
        this.in = new BufferedInputStream(in);
        this.out = new BufferedOutputStream(out);
    }

    public static byte lrc(byte[] datos) {
        byte l = 0;
        for (byte b : datos) l ^= b;
        return l;
    }

    /** Empaqueta y envia DATA. Espera ACK; si recibe NACK reintenta. */
    public void enviar(String datos) throws IOException {
        byte[] d = datos.getBytes(StandardCharsets.UTF_8);
        for (int intento = 1; intento <= MAX_REINTENTOS; intento++) {
            out.write(STX);
            out.write(d);
            out.write(ETX);
            out.write(lrc(d));
            out.flush();
            int r = in.read();
            if (r == -1) throw new EOFException("Conexion cerrada por el otro extremo");
            if (r == ACK) return;
        }
        throw new IOException("Trama rechazada (NACK) " + MAX_REINTENTOS + " veces");
    }

    /** Lee la siguiente trama valida. Contesta ACK si el LRC cuadra o NACK si no. */
    public String recibir() throws IOException {
        while (true) {
            int b;
            do {
                b = in.read();
                if (b == -1) throw new EOFException("Conexion cerrada por el otro extremo");
            } while (b != STX);

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            while ((b = in.read()) != ETX) {
                if (b == -1) throw new EOFException("Conexion cerrada a mitad de trama");
                if (b == STX) { buf.reset(); continue; }
                buf.write(b);
            }
            int lrcRecibido = in.read();
            if (lrcRecibido == -1) throw new EOFException("Conexion cerrada antes del LRC");

            byte[] d = buf.toByteArray();
            if ((byte) lrcRecibido == lrc(d)) {
                out.write(ACK);
                out.flush();
                return new String(d, StandardCharsets.UTF_8);
            }
            out.write(NACK);
            out.flush();
        }
    }

    /** Envia una peticion y devuelve la respuesta (REQUEST -> ACK -> ANSWER -> ACK). */
    public String peticion(String datos) throws IOException {
        enviar(datos);
        return recibir();
    }
}
