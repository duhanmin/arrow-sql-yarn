package com.on.yarn.arrow;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.StrUtil;

import java.util.LinkedList;

public interface Executor {

    StringBuilder logInfo = new StringBuilder();

    default String getLog() {
        String[] logDirs = System.getenv("LOG_DIRS").split(StrPool.COMMA);
        String stdout = "AppMaster.stdout";
        String stderr = "AppMaster.stderr";
        for (String logDir : logDirs) {
            String pathStdout = logDir + "/" + stdout;
            if (FileUtil.exist(pathStdout)) {
                logInfo.append(StrPool.LF).append(stdout).append(StrPool.LF);
                logInfo.append(FileUtil.readUtf8String(pathStdout));
            }
            String pathStderr = logDir + "/" + stderr;
            if (FileUtil.exist(pathStderr)) {
                logInfo.append(stderr).append(StrPool.LF);
                logInfo.append(FileUtil.readUtf8String(pathStderr));
            }
        }
        return logInfo.toString();
    }

    void run() throws Throwable;

    default LinkedList<String> init(String content) {
        LinkedList<String> sqls = new LinkedList<>();
        if (StrUtil.isNotBlank(content)) {
            for (String sql : StrUtil.split(content, ";")) {
                removePrefixSuffix(sql, " ", StrPool.TAB, StrPool.LF);
                sqls.add(sql);
            }
        }
        if (CollUtil.isEmpty(sqls)) throw new RuntimeException("sql is empty");
        return sqls;
    }

    static void removePrefixSuffix(String sql, String... strs) {
        for (String str : strs) {
            sql = StrUtil.removePrefix(sql, str);
            sql = StrUtil.removeSuffix(sql, str);
        }
    }
}
