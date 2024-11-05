package com.on.yarn.datafusion;

import com.on.yarn.arrow.Executor;

public class DatafusionExecutor implements Executor {
    private String job;

    public DatafusionExecutor(String job) {
        this.job = job;
    }

    @Override
    public void run() throws Throwable {
        String sql = DatafusionJni.sql(init(job));
        logInfo.append("\nresults: \n").append(sql);
    }

}