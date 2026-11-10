import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * Panel de monitorizacion y control de WM_Central (HTTP).
 *   GET /                -> index.html
 *   GET /api/estado      -> JSON con WS, log y ultimos riegos
 *   GET /api/orden?accion=REGAR|BLOQUEAR|ACTIVAR&ws=WS-01|ALL&segundos=N
 *   GET /api/operario?id=FO-06&nombre=Pepe   -> alta de operario
 */
public class ServidorWeb {

    public static void arrancar(int puerto) throws IOException {
        HttpServer srv = HttpServer.create(new InetSocketAddress(puerto), 0);
        srv.setExecutor(Executors.newFixedThreadPool(8));

        srv.createContext("/", ex -> {
            File f = localizarIndex();
            if (f == null) { responder(ex, 404, "text/plain", "index.html no encontrado"); return; }
            responder(ex, 200, "text/html; charset=UTF-8", Files.readString(f.toPath(), StandardCharsets.UTF_8));
        });

        srv.createContext("/api/estado", ex -> responder(ex, 200, "application/json; charset=UTF-8", estadoJson()));

        srv.createContext("/api/orden", ex -> {
            Map<String, String> q = query(ex);
            int seg;
            try { seg = Integer.parseInt(q.getOrDefault("segundos", "10")); } catch (NumberFormatException e) { seg = 10; }
            String res = WM_Central.ordenCentral(q.getOrDefault("accion", ""), q.getOrDefault("ws", ""), seg);
            responder(ex, 200, "text/plain; charset=UTF-8", res);
        });

        srv.createContext("/api/operario", ex -> {
            Map<String, String> q = query(ex);
            String id = q.getOrDefault("id", "").trim();
            String nombre = q.getOrDefault("nombre", id).trim();
            if (id.isEmpty() || id.contains("#")) { responder(ex, 400, "text/plain", "Id no valido"); return; }
            WM_Central.bd.altaOperario(id, nombre);
            WM_Central.log("[REGISTRO] Operario dado de alta: " + id + " (" + nombre + ")");
            responder(ex, 200, "text/plain; charset=UTF-8", "Operario " + id + " registrado");
        });

        srv.start();
        WM_Central.log("[HTTP] Panel web escuchando en el puerto " + puerto);
    }

    private static File localizarIndex() {
        for (String r : new String[]{"web/index.html", "WM_Central/web/index.html"}) {
            File f = new File(r);
            if (f.isFile()) return f;
        }
        return null;
    }

    private static String estadoJson() {
        StringBuilder sb = new StringBuilder("{\"kafka\":").append(WM_Central.kafkaOk).append(",\"estaciones\":[");
        synchronized (WM_Central.LOCK) {
            boolean primero = true;
            for (EstacionWS e : WM_Central.estaciones.values()) {
                if (!primero) sb.append(',');
                primero = false;
                sb.append('{')
                        .append(campo("id", e.id)).append(',')
                        .append(campo("ubicacion", e.ubicacion)).append(',')
                        .append(campo("estado", e.estado)).append(',')
                        .append(campo("detalle", e.detalle())).append(',')
                        .append(campo("operario", e.operario)).append(',')
                        .append("\"caudal\":").append(num(e.caudal)).append(',')
                        .append("\"volumen\":").append(num(e.volumen)).append(',')
                        .append("\"segundos\":").append(e.segundos).append(',')
                        .append("\"objetivo\":").append(e.objetivo).append(',')
                        .append("\"total\":").append(num(e.litrosTotales + e.volumen)).append(',')
                        .append("\"bloqueada\":").append(e.bloqueada)
                        .append('}');
            }
            sb.append("],\"operarios\":[");
            primero = true;
            long ahora = System.currentTimeMillis();
            for (Map.Entry<String, Long> o : WM_Central.operarios.entrySet()) {
                if (!primero) sb.append(',');
                primero = false;
                sb.append('{').append(campo("id", o.getKey())).append(',')
                        .append("\"conectado\":").append(WM_Central.operariosConectados.contains(o.getKey())).append(',')
                        .append("\"hace\":").append((ahora - o.getValue()) / 1000).append(',')
                        .append(campo("riego", WM_Central.riegoDe(o.getKey())))
                        .append('}');
            }
        }
        sb.append("],\"log\":[");
        synchronized (WM_Central.registro) {
            boolean primero = true;
            for (String l : WM_Central.registro) {
                if (!primero) sb.append(',');
                primero = false;
                sb.append('"').append(esc(l)).append('"');
            }
        }
        sb.append("],\"riegos\":[");
        boolean primero = true;
        for (String[] r : WM_Central.bd.ultimosRiegos(15)) {
            if (!primero) sb.append(',');
            primero = false;
            sb.append('[');
            for (int i = 0; i < r.length; i++) {
                if (i > 0) sb.append(',');
                sb.append('"').append(esc(r[i])).append('"');
            }
            sb.append(']');
        }
        return sb.append("]}").toString();
    }

    private static String campo(String k, String v) {
        return "\"" + k + "\":\"" + esc(v) + "\"";
    }

    private static String num(double d) {
        return String.format(Locale.ROOT, "%.2f", d);
    }

    private static String esc(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char ch : s.toCharArray()) {
            if (ch == '"' || ch == '\\') sb.append('\\').append(ch);
            else if (ch < 0x20) sb.append(' ');
            else sb.append(ch);
        }
        return sb.toString();
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> m = new HashMap<>();
        String q = ex.getRequestURI().getRawQuery();
        if (q == null) return m;
        for (String par : q.split("&")) {
            int i = par.indexOf('=');
            if (i > 0) m.put(URLDecoder.decode(par.substring(0, i), StandardCharsets.UTF_8),
                    URLDecoder.decode(par.substring(i + 1), StandardCharsets.UTF_8));
        }
        return m;
    }

    private static void responder(HttpExchange ex, int codigo, String tipo, String cuerpo) throws IOException {
        byte[] b = cuerpo.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", tipo);
        ex.getResponseHeaders().add("Cache-Control", "no-store");
        ex.sendResponseHeaders(codigo, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }
}
