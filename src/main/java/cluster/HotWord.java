package cluster;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import redis.clients.jedis.JedisCommands;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Created by jinha on 2017/5/11.
 */
public class HotWord  extends AbstractRedisBolt {
    public HotWord(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
//            long limit = jedisCommands.llen("cluster");
            Set<String> hwSet = new HashSet<String>();
            Map<String,Double> tf =  (Map<String, Double>) tuple.getValueByField("tf");
            for(String s :tf.keySet()) {
                if (jedisCommands.sismember("hwSet", s)) {
                    hwSet.add(s);
                }
            }
            if(hwSet.size()!=0)
                this.collector.emit(new Values(tuple.getIntegerByField("Id"),tf,hwSet));

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Id","tf","hwSet"));
    }
}
