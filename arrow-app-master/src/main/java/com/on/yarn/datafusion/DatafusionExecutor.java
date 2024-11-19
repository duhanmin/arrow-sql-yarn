package com.on.yarn.datafusion;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.on.yarn.arrow.Executor;

public class DatafusionExecutor implements Executor {
    private String job;

    public DatafusionExecutor(String job) {
        this.job = job;
    }

    @Override
    public JSONObject run() throws Throwable {
        String sql = DatafusionJni.sql(init(job));
        return JSONUtil.parseObj(sql);
    }

}