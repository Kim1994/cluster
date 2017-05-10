package utils;

import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
            return null;
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
}
