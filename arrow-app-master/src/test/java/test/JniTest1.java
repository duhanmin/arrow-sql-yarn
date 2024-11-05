package test;

import java.util.LinkedHashMap;

public class JniTest1 {
    static {
        System.loadLibrary("du_test_001");
    }

    public static void main(String[] args) {
        init();
        System.out.println("test addInt: " + (addInt(1, 2) == 3));
        JniTest1 jni = new JniTest1();
        System.out.println("test getThisField: " + (jni.getThisField("stringField", "Ljava/lang/String;") == jni.stringField));
        System.out.println("test success");
        LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap();
        linkedHashMap.put("CREATE", "CREATE EXTERNAL TABLE example STORED AS PARQUET LOCATION '/Users/duhanmin/Desktop/2/'");
        linkedHashMap.put("SELECT", "SELECT count(*) FROM example");
        String sql = sql(linkedHashMap);
        System.out.println(sql);
        divInt(1, 0);
    }

    String stringField = null;

    public static native void init();

    static native int addInt(int a, int b);

    static native int divInt(int a, int b);

    native Object getThisField(String name, String sig);

    static native String sql(LinkedHashMap<String, String> sqls);
}
