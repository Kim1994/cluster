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

/**
 * Created by jinha on 2017/4/26.
 */
public class SqlSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Connection conn = null;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.0.116:3306/test";
        String username = "root";
        String password = "han.jin";
        try {
            Class.forName(driver); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        String sql = "select article_content from article_source";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
                this.collector.emit(new Values(rs.getString(1)));
                Utils.waitForMillis(100);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}
