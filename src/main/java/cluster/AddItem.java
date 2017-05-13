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

            Map<String, Double> tf = (Map<String, Double>) tuple.getValueByField("tf");
            long n = tuple.getLongByField("classId");
            if(!jedisCommands.lindex("cluster", n).equals(tf.toString())){
                Map<String, Double> avg = json2Map(jedisCommands.lindex("cluster", n));
                int len = Integer.parseInt(jedisCommands.hget("clusterNum",""+n));
                Set<String> stringSet = new HashSet<String>(tf.keySet());
                for(String s:stringSet){
                    if (avg.containsKey(s))
                        avg.put(s, ((len * avg.get(s) + tf.get(s)) / (len + 1)));
                    else avg.put(s, tf.get(s) / (len + 1));
                    if(avg.get(s)<1E-3) {
                        avg.remove(s);
                        tf.remove(s);
                    }
                }
                jedisCommands.lset("cluster", n, avg.toString());
                jedisCommands.hincrBy("clusterNum",""+n,1);
            }

            for(String s:tf.keySet()){
                String temp = jedisCommands.hget("hotWord",s);
                if(!("," + temp + ",").contains("," + n + ""))
                    if(temp==null||temp.equals(""))
                        jedisCommands.hset("hotWord",s,""+n);
                    else
                        jedisCommands.hset("hotWord",s,temp+","+n);
            }
//            jedisCommands.hset("pear" ,tuple.getIntegerByField("Id").toString(),""+n);
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
