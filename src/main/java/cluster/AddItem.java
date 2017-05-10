package cluster;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
            jedisCommands.set("avg",""+(Integer.parseInt(jedisCommands.get("max"))/Integer.parseInt(jedisCommands.get("classify"))));

            Map<String, Double> tf = json2Map(tuple.getStringByField("tf"));

            long n = tuple.getLongByField("classId");
            if(!jedisCommands.lindex("cluster", n).equals(tuple.getStringByField("tf"))){
                Map<String, Double> avg = json2Map(jedisCommands.lindex("cluster", n));
                int len = Integer.parseInt(jedisCommands.hget("clusterNum",""+n));
                for (String s : tf.keySet()) {
                    if (avg.containsKey(s))
                        avg.put(s, ((len * avg.get(s) + tf.get(s)) / (len + 1)));
                    else avg.put(s, tf.get(s) / (len + 1));
                }
                jedisCommands.lset("cluster", n, JSONObject.fromObject(avg).toString());
                jedisCommands.hincrBy("clusterNum",""+n,1);
            }

            for(String s:tf.keySet()){
                String temp = jedisCommands.hget("hotWord",s);
                if(temp==null||temp.equals(""))
                    jedisCommands.hset("hotWord",s,""+n);
                else
                    jedisCommands.hset("hotWord",s,temp+","+n);
            }
            this.collector.emit(new Values(tuple.getIntegerByField("Id"),n));
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }



    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Id","classId"));
    }
}
