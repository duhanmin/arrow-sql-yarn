package com.on.yarn.polars;

import com.on.yarn.arrow.Executor;

public class PolarsExecutor implements Executor {

    private String job;

    public PolarsExecutor(String job) {
        this.job = job;
    }

    @Override
    public void run() throws Throwable {
        String sql = PolarsJni.sql(init(job));
        logInfo.append("\nresults: \n").append(sql);
    }
}
