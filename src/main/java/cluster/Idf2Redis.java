package cluster;



import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import redis.clients.jedis.JedisCommands;

import java.util.*;

/**
 * Created by jinha on 2017/3/27.
 */
public class Idf2Redis extends AbstractRedisBolt {

    public Idf2Redis(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;
        try{
            jedisCommands = getInstance();
            jedisCommands.incrBy("sentencenum" ,1);
            int sNum = Integer.parseInt(jedisCommands.get("sentencenum"));
            List<String> word = Arrays.asList(tuple.getStringByField("words").split(","));
            List<String> tempList = new ArrayList<String>(new HashSet<String>(word));
            for (String s:tempList) {
//                double wNum;
//                String tempString = jedisCommands.hget("word",s);
//                if(tempString==null||tempString.equals(""))
//                    wNum = 0;
//                else wNum = Double.parseDouble(tempString);
                jedisCommands.hincrBy("word",s,1);
            }
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
