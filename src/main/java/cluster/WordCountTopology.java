package cluster;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.redis.common.config.JedisPoolConfig;

public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("192.168.0.101").setPort(6379).build();
//        RedisStoreMapper storeMapper = new Idf2Redis();
//        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper) ;

        builder.setSpout("Spout", new SqlSpout());
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt("Split",new SplitSentenceBolt(),4).shuffleGrouping("Spout");
        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt("Tfidf", new Tfidf(poolConfig)).shuffleGrouping("Split");
        builder.setBolt("Idf2Redis",  new Idf2Redis(poolConfig)).shuffleGrouping("Split");
//        builder.setBolt("Report",new ReportBolt()).shuffleGrouping("Tfidf");
        builder.setBolt("Classify", new Classify(poolConfig),6).shuffleGrouping("Tfidf");
        builder.setBolt("AddItem", new AddItem(poolConfig),2).shuffleGrouping("Classify");

        Config config = new Config();
//        config.setDebug(true);
//        config.setMaxTaskParallelism(8);
//
//        LocalCluster cluster = new LocalCluster();
//
//        cluster.submitTopology("cluster", config, builder.createTopology());

        config.setNumWorkers(8);
        StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
    }
}
