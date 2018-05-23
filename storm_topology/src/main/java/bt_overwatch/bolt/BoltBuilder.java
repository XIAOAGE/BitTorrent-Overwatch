package bt_overwatch.bolt;

import java.util.Properties;

import bt_overwatch.Keys;

public class BoltBuilder {

    public Properties configs = null;

    public BoltBuilder(Properties configs) {
        this.configs = configs;
    }

    public SinkTypeBolt buildSinkTypeBolt() {
        return new SinkTypeBolt();
    }

    public MongodbBolt buildMongodbBolt() {
        String host = configs.getProperty(Keys.MONGO_HOST);
        int port = Integer.parseInt(configs.getProperty(Keys.MONGO_PORT));
        String db = configs.getProperty(Keys.MONGO_DATABASE);
        String collection = configs.getProperty(Keys.MONGO_COLLECTION);
        return new MongodbBolt(host, port, db, collection);
    }

}