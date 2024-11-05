package test;

import java.util.LinkedHashMap;

public class JniTest {
    static {
        System.loadLibrary("datafusion_jni");
    }

    public static void main(String[] args) {
        LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap();
        linkedHashMap.put("CREATE", "CREATE EXTERNAL TABLE example STORED AS PARQUET LOCATION '/Users/duhanmin/Desktop/2/'");
        linkedHashMap.put("SELECT", "SELECT count(*) FROM example");
        String sql = sql(linkedHashMap);
        System.out.println(sql);
    }

    static native String sql(LinkedHashMap<String, String> sqls);
}
