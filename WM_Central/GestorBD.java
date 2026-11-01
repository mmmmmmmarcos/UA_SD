import java.sql.*;

public class GestorBD {
    private static final String URL = "jdbc:sqlite:riego_sd.db";

    public static synchronized void inicializarBD() {
        try {
            Class.forName("org.sqlite.JDBC");
            try (Connection conn = DriverManager.getConnection(URL);
                 Statement stmt = conn.createStatement()) {

                stmt.execute("CREATE TABLE IF NOT EXISTS estaciones (id TEXT PRIMARY KEY, ubicacion TEXT DEFAULT 'ZonaNorte', estado TEXT NOT NULL)");
                stmt.execute("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY, nombre TEXT NOT NULL)");
                stmt.execute("CREATE TABLE IF NOT EXISTS historico_riegos (id INTEGER PRIMARY KEY AUTOINCREMENT, id_estacion TEXT, duracion INTEGER, litros REAL, operario TEXT DEFAULT 'FO-01', fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

                // Migración suave por si la tabla antigua ya existe en el volumen sin columna ubicacion u operario
                try { stmt.execute("ALTER TABLE estaciones ADD COLUMN ubicacion TEXT DEFAULT 'ParqueCentral'"); } catch (SQLException ignored) {}
                try { stmt.execute("ALTER TABLE historico_riegos ADD COLUMN operario TEXT DEFAULT 'FO-01'"); } catch (SQLException ignored) {}

                stmt.execute("INSERT OR IGNORE INTO estaciones (id, ubicacion, estado) VALUES ('WS-01', 'ZonaNorte', 'DISCONNECTED')");
                stmt.execute("INSERT OR IGNORE INTO estaciones (id, ubicacion, estado) VALUES ('WS-02', 'ParqueCentral', 'DISCONNECTED')");
                stmt.execute("INSERT OR IGNORE INTO estaciones (id, ubicacion, estado) VALUES ('WS-03', 'JardinSur', 'DISCONNECTED')");
                stmt.execute("INSERT OR IGNORE INTO operarios (id, nombre) VALUES ('FO-01', 'Operario Campo 1')");
                stmt.execute("INSERT OR IGNORE INTO operarios (id, nombre) VALUES ('FO-02', 'Operario Campo 2')");

                // Al reiniciar WM_Central, todas las estaciones pasan a DISCONNECTED hasta que su Monitor conecte
                stmt.execute("UPDATE estaciones SET estado = 'DISCONNECTED'");

                System.out.println("\n=== [SQLITE] ESTACIONES REGISTRADAS EN WM_CENTRAL AL INICIO ===");
                try (ResultSet rs = stmt.executeQuery("SELECT id, ubicacion, estado FROM estaciones")) {
                    while (rs.next()) {
                        System.out.println(" -> Estación: " + rs.getString("id") +
                                           " | Ubicación: " + rs.getString("ubicacion") +
                                           " | Estado inicial: " + rs.getString("estado") + " (Esperando conexión de Monitor)");
                    }
                }
                System.out.println("===============================================================\n");
            }
        } catch (Exception e) {
            System.err.println("[SQLITE] Error inicializando BD: " + e.getMessage());
        }
    }

    public static synchronized void registrarEstacion(String idEstacion, String ubicacion, String nuevoEstado) {
        String sql = "INSERT INTO estaciones (id, ubicacion, estado) VALUES (?, ?, ?) " +
                     "ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion, estado = excluded.estado";
        try (Connection conn = DriverManager.getConnection(URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, idEstacion);
            pstmt.setString(2, ubicacion);
            pstmt.setString(3, nuevoEstado);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error registrando estación: " + e.getMessage());
        }
    }

    public static synchronized void actualizarEstadoEstacion(String idEstacion, String nuevoEstado) {
        String sql = "INSERT INTO estaciones (id, ubicacion, estado) VALUES (?, 'ZonaNorte', ?) " +
                     "ON CONFLICT(id) DO UPDATE SET estado = excluded.estado";
        try (Connection conn = DriverManager.getConnection(URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, idEstacion);
            pstmt.setString(2, nuevoEstado);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error actualizando estado: " + e.getMessage());
        }
    }

    public static synchronized String obtenerEstadoEstacion(String idEstacion) {
        String sql = "SELECT estado FROM estaciones WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, idEstacion);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("estado");
            }
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error consultando estación: " + e.getMessage());
        }
        return "UNKNOWN";
    }

    public static synchronized void registrarResumen(String tramaResumen) {
        // Formato esperado: WS-01#segundos#litros[#operario]
        String[] partes = tramaResumen.split("#");
        if (partes.length < 3) return;
        String operario = (partes.length >= 4) ? partes[3] : "FO-01";

        String sql = "INSERT INTO historico_riegos (id_estacion, duracion, litros, operario) VALUES (?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, partes[0]);
            pstmt.setInt(2, Integer.parseInt(partes[1]));
            pstmt.setDouble(3, Double.parseDouble(partes[2]));
            pstmt.setString(4, operario);
            pstmt.executeUpdate();
            System.out.println("[SQLITE] Resumen guardado en BD -> Estación: " + partes[0] +
                               " | Segundos: " + partes[1] + " | Litros: " + partes[2] + " | Operario: " + operario);
        } catch (Exception e) {
            System.err.println("[SQLITE] Error guardando resumen: " + e.getMessage());
        }
    }
}