package com.on.yarn.datafusion;

import com.on.yarn.admin.RoutSQL;
import com.on.yarn.arrow.Executor;

public class DatafusionExecutor implements Executor {
    private String job;

    public DatafusionExecutor(String job) {
        this.job = job;

    }

    @Override
    public void run() throws Throwable {
        String result = DatafusionJni.sql(init(job));
        logInfo.append("\nresults: \n").append(result);
        RoutSQL.result(result);
    }

}