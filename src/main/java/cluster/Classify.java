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
        int count = 0;
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            jedisCommands.incrBy("classify" ,1);
            Map<String,Double> tf =  (Map<String, Double>) tuple.getValueByField("tf");
            double min = 0;
            long minClass = -1;

            String classList = tuple.getStringByField("classList").replaceAll("[\\[\\]\\s]","");
            if(!(classList==null||classList.equals(""))){
                String[] tempCList = classList.split(",");
                for (String s:tempCList){
                    count++;
                    double dis = getCosDistance(tf, JSONObject.fromObject(jedisCommands.lindex("cluster",Long.parseLong(s))));
                    if(dis>0.2&&dis>min)
                        minClass = Long.parseLong(s);
                }
            }

            Map<String,Double> temp;
            Random random = new Random();
            int time = 30;
            for(long i = tuple.getLongByField("limit") ;;i++){
                temp = JSONObject.fromObject(jedisCommands.lindex("cluster",i));
                if(temp.size()==0){
                    if(time < 15)
                        break;
                    else {
                        waitForMillis(time /= 2);
                        i--;
                        continue;
                    }
                }
                count++;
                double dis = getCosDistance(tf,temp);
                if(dis>0.2&&dis>min)
                    minClass = i;
            }
            if(minClass == -1){
                minClass = jedisCommands.rpush("cluster",tf.toString())-1;
                jedisCommands.hset("clusterNum",""+minClass,"1");
            }

            jedisCommands.incrBy("max",count);
            this.collector.emit(new Values(tuple.getIntegerByField("Id"),tf,minClass));

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Id","tf","classId"));
    }
}
