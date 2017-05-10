package cluster;



import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.JedisCommands;
import utils.Utils;


import java.util.*;

import static utils.Utils.json2Map;

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


//            Map<String ,String> valList = jedisCommands.hgetAll("cluster");

            Map<String,Double> tf =  json2Map(tuple.getStringByField("tf"));
//
//
//            int len = valList.size();
//            boolean flag = true;
//            for (int i = 0; i < len; i++) {
//                double dis = this.getCosDistance(json2Map(valList.get("" + i)),tf);
//                if(dis>0.3){
//                    flag = false;
//                    this.collector.emit(new Values(tuple.getStringByField("sentence"),tuple.getStringByField("tf"),""+i));
//                }
//
//            }
//            if(flag){
//                jedisCommands.hset("cluster",""+jedisCommands.hlen("cluster"),tuple.getStringByField("tf"));
//                jedisCommands.lpush(""+jedisCommands.hlen("cluster"),tuple.getStringByField("tf"));
//            }
            boolean flag = true;
            Map<String,Double> temp;
            int time = 400;
            for(long i = 0 ;;i++){
                temp = json2Map(jedisCommands.lindex("cluster",i));
                if(temp == null){
                    if(time == 50)
                        break;
                    else Utils.waitForMillis(time/=2);
                }
                else{
                    double dis = this.getCosDistance(temp,tf);
                    if(dis>0.6){
                        flag = false;
                        this.collector.emit(new Values(tuple.getStringByField("sentence"),tuple.getStringByField("tf"),i));
                    }
                }
            }

            if(flag){
                long n = jedisCommands.rpush("cluster",tuple.getStringByField("tf"));
                jedisCommands.lpush("" + (n-1),tuple.getStringByField("sentence"));
            }
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }


    private Double getCosDistance(Map<String, Double> aMap, Map<String, Double> bMap){
        Set<String> set = new HashSet<String>();
        set.addAll(aMap.keySet());
        set.addAll(bMap.keySet());
        double re = 0;
        for (String s:set) {
            double a = aMap.containsKey(s)?aMap.get(s):0.0;
            double b = bMap.containsKey(s)?bMap.get(s):0.0;
            re+=a*b;
        }
        double aPlus = 0;
        for(double d:aMap.values()){
            aPlus+=d*d;
        }

        double bPlus = 0;
        for(double d:bMap.values()){
            bPlus+=d*d;
        }

        return re/Math.sqrt(aPlus)/Math.sqrt(bPlus);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence","tf","class"));
    }
}
