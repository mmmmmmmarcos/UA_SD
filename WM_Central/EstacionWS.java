/**
 * Estado en memoria de una Watering Station tal y como la ve WM_Central.
 * Todos los accesos se hacen con el lock de WM_Central (WM_Central.LOCK).
 */
public class EstacionWS {
    public static final String AVAILABLE = "AVAILABLE";
    public static final String WATERING = "WATERING";
    public static final String LEAK = "LEAK";
    public static final String OUT_OF_SERVICE = "OUT_OF_SERVICE";
    public static final String DISCONNECTED = "DISCONNECTED";

    /** Salud que reporta el monitor cada segundo. */
    public static final String SALUD_OK_IDLE = "OK_IDLE";
    public static final String SALUD_OK_REGANDO = "OK_REGANDO";
    public static final String SALUD_FUGA = "FUGA";
    public static final String SALUD_ENGINE_KO = "ENGINE_KO";

    final String id;
    String ubicacion;
    String estado = DISCONNECTED;
    boolean bloqueada;
    double litrosTotales;

    // Conexion del monitor
    HiloServidor monitor;          // hilo del monitor autenticado (null = desconectada)
    String salud = SALUD_ENGINE_KO;

    // Riego en curso
    String idRiego = "";
    String operario = "";
    int segundos;
    int objetivo;
    double caudal;
    double volumen;
    long pendienteDesde;           // >0 mientras esperamos que la WS confirme el inicio
    long ultimaActividad;          // ultimo INICIO/TEL recibido del riego en curso

    EstacionWS(String id, String ubicacion) {
        this.id = id;
        this.ubicacion = ubicacion;
    }

    boolean tieneRiego() {
        return !idRiego.isEmpty();
    }

    void limpiarRiego() {
        idRiego = "";
        operario = "";
        segundos = 0;
        objetivo = 0;
        caudal = 0;
        volumen = 0;
        pendienteDesde = 0;
        ultimaActividad = 0;
    }

    /**
     * Tabla de estados (el monitor es quien informa de la salud):
     *   Monitor caido                    -> DISCONNECTED (gris)
     *   Monitor OK + Engine KO / fuga    -> LEAK (rojo)
     *   Bloqueada por Central            -> OUT_OF_SERVICE (naranja)
     *   Regando o riego recien autorizado-> WATERING (verde parpadeante)
     *   Resto                            -> AVAILABLE (verde)
     */
    String calcularEstado() {
        if (monitor == null) return DISCONNECTED;
        if (salud.isEmpty() || SALUD_FUGA.equals(salud) || SALUD_ENGINE_KO.equals(salud)) return LEAK;
        if (bloqueada) return OUT_OF_SERVICE;
        // Ocupada mientras haya un riego autorizado o en curso (aunque el monitor aun no lo haya reportado)
        if (SALUD_OK_REGANDO.equals(salud) || pendienteDesde > 0 || tieneRiego()) return WATERING;
        return AVAILABLE;
    }

    String detalle() {
        switch (estado) {
            case WATERING:
                if (pendienteDesde > 0 && segundos == 0) return "Autorizado, abriendo valvula... (Op: " + operario + ")";
                return String.format(java.util.Locale.ROOT, "%.1f L/min | %.2f L | %d/%d s | Op: %s",
                        caudal, volumen, segundos, objetivo, operario);
            case LEAK:
                return SALUD_ENGINE_KO.equals(salud) ? "AVERIADA: engine sin respuesta" : "FUGA detectada";
            case OUT_OF_SERVICE:
                return "Out of Service";
            case DISCONNECTED:
                return "Desconectada";
            default:
                return "Disponible";
        }
    }
}
