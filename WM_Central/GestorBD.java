import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Acceso a la base de datos SQLite de WM_Central.
 * Una unica conexion compartida y todos los metodos synchronized: SQLite solo admite
 * un escritor a la vez, asi evitamos el error "database is locked".
 */
public class GestorBD {
    private final Connection conn;

    public GestorBD(String ruta) throws SQLException {
        File f = new File(ruta).getAbsoluteFile();
        if (f.getParentFile() != null) f.getParentFile().mkdirs();
        conn = DriverManager.getConnection("jdbc:sqlite:" + f.getPath());

        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS estaciones (" +
                    "id TEXT PRIMARY KEY, ubicacion TEXT NOT NULL, estado TEXT NOT NULL DEFAULT 'DISCONNECTED')");
            try { st.execute("ALTER TABLE estaciones ADD COLUMN bloqueada INTEGER NOT NULL DEFAULT 0"); }
            catch (SQLException yaExiste) { /* columna ya creada en un arranque anterior */ }

            st.execute("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY, nombre TEXT NOT NULL)");
            st.execute("CREATE TABLE IF NOT EXISTS riegos (" +
                    "id_riego TEXT PRIMARY KEY, id_estacion TEXT NOT NULL, operario TEXT, " +
                    "segundos INTEGER, litros REAL, motivo TEXT, fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            // Datos iniciales (solo se insertan la primera vez)
            st.execute("INSERT OR IGNORE INTO estaciones (id, ubicacion) VALUES " +
                    "('WS-01','Parque Norte'),('WS-02','Jardin Central'),('WS-03','Parque Sur')");
            st.execute("INSERT OR IGNORE INTO operarios (id, nombre) VALUES " +
                    "('FO-01','Operario 1'),('FO-02','Operario 2'),('FO-03','Operario 3')," +
                    "('FO-04','Operario 4'),('FO-05','Operario 5')");

            // Al (re)arrancar no conocemos el estado real de ninguna WS hasta que su monitor conecte
            st.execute("UPDATE estaciones SET estado = 'DISCONNECTED'");
        }
    }

    /** Devuelve [id, ubicacion, bloqueada(0/1), litrosTotales] de cada WS registrada. */
    public synchronized List<String[]> cargarEstaciones() throws SQLException {
        List<String[]> lista = new ArrayList<>();
        String sql = "SELECT e.id, e.ubicacion, e.bloqueada, " +
                "COALESCE((SELECT SUM(litros) FROM riegos r WHERE r.id_estacion = e.id), 0) AS total " +
                "FROM estaciones e ORDER BY e.id";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                lista.add(new String[]{rs.getString(1), rs.getString(2),
                        String.valueOf(rs.getInt(3)), String.valueOf(rs.getDouble(4))});
            }
        }
        return lista;
    }

    public synchronized void registrarEstacion(String id, String ubicacion) {
        ejecutar("INSERT INTO estaciones (id, ubicacion) VALUES (?, ?) " +
                "ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion", id, ubicacion);
    }

    public synchronized void actualizarEstado(String id, String estado) {
        ejecutar("UPDATE estaciones SET estado = ? WHERE id = ?", estado, id);
    }

    public synchronized void setBloqueada(String id, boolean bloqueada) {
        ejecutar("UPDATE estaciones SET bloqueada = ? WHERE id = ?", bloqueada ? "1" : "0", id);
    }

    public synchronized boolean existeOperario(String id) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM operarios WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error consultando operario: " + e.getMessage());
            return false;
        }
    }

    public synchronized void altaOperario(String id, String nombre) {
        ejecutar("INSERT INTO operarios (id, nombre) VALUES (?, ?) " +
                "ON CONFLICT(id) DO UPDATE SET nombre = excluded.nombre", id, nombre);
    }

    /** Guarda un riego finalizado. Devuelve false si ya estaba guardado (mensaje duplicado). */
    public synchronized boolean guardarRiego(String idRiego, String ws, String operario,
                                             int segundos, double litros, String motivo) {
        // La fecha se pone desde Java (hora local, igual que los logs): CURRENT_TIMESTAMP de SQLite va en UTC
        String sql = "INSERT OR IGNORE INTO riegos (id_riego, id_estacion, operario, segundos, litros, motivo, fecha) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(7, java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            ps.setString(1, idRiego);
            ps.setString(2, ws);
            ps.setString(3, operario);
            ps.setInt(4, segundos);
            ps.setDouble(5, litros);
            ps.setString(6, motivo);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error guardando riego: " + e.getMessage());
            return false;
        }
    }

    /** Ultimos n riegos: [fecha, ws, operario, segundos, litros, motivo]. */
    public synchronized List<String[]> ultimosRiegos(int n) {
        List<String[]> lista = new ArrayList<>();
        String sql = "SELECT fecha, id_estacion, operario, segundos, litros, motivo FROM riegos " +
                "ORDER BY fecha DESC LIMIT ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, n);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    lista.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3),
                            String.valueOf(rs.getInt(4)), String.format(java.util.Locale.ROOT, "%.2f", rs.getDouble(5)),
                            rs.getString(6)});
                }
            }
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error leyendo riegos: " + e.getMessage());
        }
        return lista;
    }

    private void ejecutar(String sql, String... params) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setString(i + 1, params[i]);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[SQLITE] Error: " + e.getMessage());
        }
    }
}
