package cluster;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.JedisCommands;

import java.util.*;

/**
 * Created by jinha on 2017/3/19.
 *
 */
public class Tfidf extends AbstractRedisBolt {

    public Tfidf(JedisPoolConfig config) {super(config);}

    public void execute(Tuple tuple) {

        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            jedisCommands.incrBy("tf",1);
            List<String> word = Arrays.asList(tuple.getStringByField("words").split(","));
            Map<String,Double> tempMap  = new HashMap<String, Double>();
            for (String w:word) {
                if(tempMap.get(w)==null)
                    tempMap.put(w,1.0/word.size());
                else tempMap.put(w,(1+tempMap.get(w)*word.size())/word.size());
            }
            jedisCommands.hset("tflist",tuple.getIntegerByField("Id").toString(), tempMap.toString());
            this.collector.emit(new Values(tuple.getIntegerByField("Id"), tempMap));
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Id","tf"));
    }
}
