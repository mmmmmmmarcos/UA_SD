import java.sql.*;

public class GestorBD {
    private static final String URL = "jdbc:sqlite:riego_sd.db";

    public static synchronized void inicializarBD() {
        try {
            Class.forName("org.sqlite.JDBC"); // <-- LÍNEA CLAVE
            try (Connection conn = DriverManager.getConnection(URL);
                 Statement stmt = conn.createStatement()) {
                
                stmt.execute("CREATE TABLE IF NOT EXISTS estaciones (id TEXT PRIMARY KEY, estado TEXT NOT NULL)");
                stmt.execute("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY, nombre TEXT NOT NULL)");
                stmt.execute("CREATE TABLE IF NOT EXISTS historico_riegos (id INTEGER PRIMARY KEY AUTOINCREMENT, id_estacion TEXT, duracion INTEGER, litros REAL, fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

                stmt.execute("INSERT OR IGNORE INTO estaciones (id, estado) VALUES ('WS-01', 'AVAILABLE')");
                stmt.execute("INSERT OR IGNORE INTO estaciones (id, estado) VALUES ('WS-02', 'AVAILABLE')");
                stmt.execute("INSERT OR IGNORE INTO operarios (id, nombre) VALUES ('OP-01', 'Operario Campo 1')");

                System.out.println("[SQLITE] Base de datos inicializada correctamente.");
            }
        } catch (Exception e) {
            System.err.println("[SQLITE] Error inicializando BD: " + e.getMessage());
        }
    }
    public static synchronized void actualizarEstadoEstacion(String idEstacion, String nuevoEstado) {
        String sql = "INSERT INTO estaciones (id, estado) VALUES (?, ?) " +
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
        // Formato esperado: WS-01#segundos#litros
        String[] partes = tramaResumen.split("#");
        if (partes.length < 3) return;

        String sql = "INSERT INTO historico_riegos (id_estacion, duracion, litros) VALUES (?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, partes[0]);
            pstmt.setInt(2, Integer.parseInt(partes[1]));
            pstmt.setDouble(3, Double.parseDouble(partes[2]));
            pstmt.executeUpdate();
            System.out.println("[SQLITE] Resumen guardado en BD -> Estación: " + partes[0] +
                               " | Segundos: " + partes[1] + " | Litros: " + partes[2]);
        } catch (Exception e) {
            System.err.println("[SQLITE] Error guardando resumen: " + e.getMessage());
        }
    }
}