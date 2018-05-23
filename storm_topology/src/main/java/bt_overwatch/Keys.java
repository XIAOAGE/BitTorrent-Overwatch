package bt_overwatch;

public class Keys {


    public static final String TOPOLOGY_NAME                  = "topology";

    //kafka spout
    public static final String KAFKA_SPOUT_ID = "kafka-spout";
    public static final String KAFKA_ZOOKEEPER               = "kafka.zookeeper";
    public static final String KAFKA_TOPIC              = "kafa.topic";
    public static final String KAFKA_ZKROOT                    = "kafka.zkRoot";
    public static final String KAFKA_CONSUMERGROUP     = "kafka.consumer.group";
    public static final String KAFKA_SPOUT_COUNT          = "kafkaspout.count";

    //sink bolt
    public static final String SINK_TYPE_BOLT_ID = "sink-type-bolt";
    public static final String SINK_BOLT_COUNT = "sinkbolt.count";

    //mongodb bolt
    public static final String MONGO_BOLT_ID = "mongodb.bolt.id";
    public static final String MONGO_HOST = "mongodb.host";
    public static final String MONGO_PORT = "mongodb.port";
    public static final String MONGO_DATABASE = "mongodb.database";
    public static final String MONGO_COLLECTION = "mongodb.collection";
    public static final String MONGO_BOLT_COUNT = "mongodbbolt.count";

}