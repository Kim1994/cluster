package cluster;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
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
 * Created by jinha on 2017/4/26.
 */
public class SqlSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Connection conn = null;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        conn = getConn();
    }

    public void nextTuple() {
        String sql = "select articleid , article_content from cs_article_search";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                if(!(rs.getString(2).equals("")||rs.getString(2)==null))
                    this.collector.emit(new Values(rs.getInt(1),rs.getString(2)));
                    Utils.waitForMillis(60);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Id","sentence"));
    }
}
