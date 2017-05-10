package cluster;


import java.io.*;
import java.util.Map;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import utils.Utils;

public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    BufferedReader br =null;
    String line =null;
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
//            br=new BufferedReader(new InputStreamReader(new FileInputStream("/root/corpus.txt"),"GBK"));
            br=new BufferedReader(new InputStreamReader(new FileInputStream("d://corpus.txt"),"GBK"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        while(true){
            try {
                line =br.readLine();
                if(line ==null){
                    br=new BufferedReader(new InputStreamReader(new FileInputStream("d://corpus.txt"),"GBK"));
                    line = br.readLine();
                }
                this.collector.emit(new Values(line.split("【")[0]));
                Utils.waitForMillis(100);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
