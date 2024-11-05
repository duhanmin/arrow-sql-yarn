package com.on.yarn.job;

import cn.hutool.json.JSONUtil;
import lombok.Data;

@Data
public class YarnJob {
    private String appId;
    private String appState;
    private Long date;

    public YarnJob(String appId, String appState) {
        this.appId = appId;
        this.appState = appState;
        this.date = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }
}
