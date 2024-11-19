package com.on.yarn.polars;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.on.yarn.arrow.Executor;

public class PolarsExecutor implements Executor {

    private String job;

    public PolarsExecutor(String job) {
        this.job = job;
    }

    @Override
    public JSONObject run() throws Throwable {
        String sql = PolarsJni.sql(init(job));
        return JSONUtil.parseObj(sql);
    }
}
