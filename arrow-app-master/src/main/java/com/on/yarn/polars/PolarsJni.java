package com.on.yarn.polars;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.LinkedList;

public class PolarsJni {

    private static final Log log = LogFactory.getLog(PolarsJni.class);

    static {
        log.info("load polars jni");
        JNILoader.load();
    }

    static native String sql(LinkedList<String> sqls);

    public static void main(String[] args) {
        LinkedList<String> linkedHashMap = new LinkedList();
        linkedHashMap.add("SELECT count(*) FROM read_parquet('/Users/duhanmin/Desktop/2/*.parquet') limit 10");
        linkedHashMap.add("CREATE TABLE table_name AS SELECT count(*) FROM read_parquet('/Users/duhanmin/Desktop/2/*.parquet') limit 10");

        String sql = sql(linkedHashMap);
        System.out.println(sql);
    }
}
