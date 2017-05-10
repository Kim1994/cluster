package cluster;




import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import net.sf.json.JSONObject;


import java.util.*;

public class ReportBolt extends BaseRichBolt {


    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    }

    public void execute(Tuple tuple) {
//        GsonBuilder gb = new GsonBuilder();
//        Gson g = gb.create();
//        Map<String,Double> tf =  g.fromJson(tuple.getStringByField("tf"), new TypeToken<Map<String, Double>>() {}.getType());
        Map<String,Double> tf = JSONObject.fromObject(tuple.getStringByField("tf"));
        System.out.println(tuple.getStringByField("sentence")+"  " +tf.toString());

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }
}
