package cluster;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import net.sf.json.JSONObject;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.JedisCommands;


import java.util.Map;

import static utils.Utils.json2Map;

/**
 * Created by jinha on 2017/3/29.
 *
 */
public class AddItem extends AbstractRedisBolt {
    public AddItem(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            jedisCommands.incrBy("add" ,1);

            long n = tuple.getLongByField("class");

            Map<String ,Double> avg = json2Map(jedisCommands.lindex("cluster", n));
            long len = jedisCommands.llen("" + n);
            Map<String,Double> tf = json2Map(tuple.getStringByField("tf"));

            for (String s:tf.keySet()) {
                if(avg.containsKey(s))
                    avg.put(s,((len*avg.get(s)+tf.get(s))/(len+1)));
                else avg.put(s,tf.get(s)/(len+1));
            }

            jedisCommands.lset("cluster", n ,JSONObject.fromObject(avg).toString());
            jedisCommands.lpush("" + n,tuple.getStringByField("sentence"));
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }



    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
