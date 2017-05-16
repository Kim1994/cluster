package cluster;



import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.IFn;
import net.sf.json.JSONObject;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.JedisCommands;

import java.util.*;

import static utils.Utils.getCosDistance;
import static utils.Utils.waitForMillis;

/**
 * Created by jinha on 2017/3/29.
 *
 */
public class Classify extends AbstractRedisBolt{

    Classify(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            jedisCommands.incrBy("classify" ,1);
            Map<String,Double> tf =  (Map<String, Double>) tuple.getValueByField("tf");
            double min = 0;
            long minClass = -1;
            Map<String ,Long> hwMap = new HashMap<String, Long>();
            for(String s : (Set<String>)tuple.getValueByField("hwSet"))
                hwMap.put(s, (long) 0);
            boolean flag = true;
            List<String> classSet = new ArrayList<String>();
            while (flag){
                for(String s:hwMap.keySet()) {
                    long len = jedisCommands.scard(s);
                    if (len-1 > hwMap.get(s)) {
                        Set<String> tempList = jedisCommands.smembers(s);
                        for(String cString:tempList) {
                            if(!classSet.contains(cString)){
                                classSet.add(cString);
                                double dis = getCosDistance(tf,JSONObject.fromObject(jedisCommands.lindex("cluster",Long.parseLong(cString))));
                                if(dis>0.3&&dis>min) {
                                    minClass = Long.parseLong(cString);
                                    min = dis;
                                }
                            }
                        }
                        hwMap.put(s,len-1);
                        flag = true;
                    }
                    else flag = false;
                }
            }
            int isNew = 0;
            if(minClass ==-1){
                minClass = jedisCommands.rpush("cluster",tf.toString())-1;
                jedisCommands.hset("clusterNum",""+minClass,"1");
                isNew = 1;
            }
            for(String s:hwMap.keySet()) {
                jedisCommands.sadd(s,""+minClass);
            }
            this.collector.emit(new Values(tuple.getIntegerByField("Id"),tf,minClass,isNew,tuple.getValueByField("hwSet")));

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Id","tf","classId","isNew","hwSet"));
    }
}
