package com.on.yarn.datafusion;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.LinkedList;

final class DatafusionJni {

    private static final Log log = LogFactory.getLog(DatafusionExecutor.class);

    static {
        log.info("load datafusion jni");
        JNILoader.load();
    }

    static native String sql(LinkedList<String> sqls);

    public static void main(String[] args) {
        LinkedList<String> linkedHashMap = new LinkedList();
        linkedHashMap.add("CREATE EXTERNAL TABLE example STORED AS PARQUET LOCATION '/Users/duhanmin/Desktop/2/'");
        linkedHashMap.add("SELECT count(*) FROM example");
        DatafusionJni datafusionJni = new DatafusionJni();
        String sql = datafusionJni.sql(linkedHashMap);
        System.out.println(sql);
    }
}