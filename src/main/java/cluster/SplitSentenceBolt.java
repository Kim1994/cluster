package cluster;



import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apdplat.word.segmentation.SegmentationAlgorithm.MinimumMatching;

public class SplitSentenceBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        List<String> strings = new ArrayList<String>();
        String sentence = tuple.getStringByField("sentence").replaceAll("[^,.，。\\u4e00-\\u9fa5]", "");
        if(!(sentence == null ||sentence.equals(""))) {
            List<Word> words = WordSegmenter.seg(sentence,MinimumMatching);

//            JiebaSegmenter segmenter = new JiebaSegmenter();
            this.collector.emit(new Values(tuple.getIntegerByField("Id"), words.toString().replaceAll("[\\[\\]\\s]","")));

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Id","words"));
    }
}
