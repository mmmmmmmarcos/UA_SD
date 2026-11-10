import java.util.Properties;

/** Configuracion comun de productores y consumidores Kafka (igual en todos los modulos). */
public final class KafkaUtil {
    private KafkaUtil() { }

    public static Properties propsProductor(String broker) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("acks", "all");
        p.put("max.block.ms", "5000");          // no bloquear la aplicacion si el broker cae
        p.put("delivery.timeout.ms", "30000");
        p.put("request.timeout.ms", "10000");
        p.put("linger.ms", "0");
        return p;
    }

    public static Properties propsConsumidor(String broker, String grupo, String offsetReset) {
        return propsConsumidor(broker, grupo, offsetReset, null);
    }

    /**
     * instanciaFija != null -> "static membership": si el proceso cae y vuelve con el mismo id,
     * Kafka le devuelve sus particiones al instante (sin esperar a que caduque la sesion).
     */
    public static Properties propsConsumidor(String broker, String grupo, String offsetReset, String instanciaFija) {
        Properties p = new Properties();
        if (instanciaFija != null) p.put("group.instance.id", instanciaFija);
        p.put("session.timeout.ms", "10000");
        p.put("heartbeat.interval.ms", "3000");
        p.put("bootstrap.servers", broker);
        p.put("group.id", grupo);
        p.put("auto.offset.reset", offsetReset);
        p.put("enable.auto.commit", "true");
        p.put("auto.commit.interval.ms", "1000");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
