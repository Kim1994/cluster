package utils;

import com.mysql.jdbc.Connection;
import net.sf.json.JSONObject;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class Utils {

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void waitForMillis(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
        }
    }
    public static Map<String,Double> json2Map(String s){
        if(s == null||s.equals("")){
            return new HashMap<String, Double>();
        }
        Map<String,Double> re = new HashMap<String, Double>();
        JSONObject js = JSONObject.fromObject(s);
        Iterator it = js.keys();
        while(it.hasNext()){
            String key = String.valueOf(it.next());
            double value = Double.valueOf((js.get(key).toString()));
            re.put(key,value);
        }
        return re;
    }
    public static Double getCosDistance(Map<String, Double> aMap, Map<String, Double> bMap){
        Set<String> set = new HashSet<String>();
        set.addAll(aMap.keySet());
        set.retainAll(bMap.keySet());
        if(set.size()<bMap.size()*0.15)
            return 0.0;
        double re = 0;
        for (String s:set) {
            double a = aMap.get(s);
//            double a = aMap.containsKey(s)?aMap.get(s):0.0;
            double b = bMap.get(s);
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

    public static Connection getConn(){
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/test";
        String username = "root";
        String password = "han.jin";
        try {
            return  (Connection) DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
