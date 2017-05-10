package cluster;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import utils.Utils;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static utils.Utils.getConn;

/**
 * Created by jinha on 2017/5/11.
 */
public class SqlSave extends BaseRichBolt {
    private OutputCollector collector;
    private Connection conn = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        conn = getConn();
    }

    public void execute(Tuple tuple) {
        String sql = "UPDATE article_source SET clusterID = "+tuple.getLongByField("classId")+" WHERE articleid = " +tuple.getIntegerByField("Id");
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement)conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
