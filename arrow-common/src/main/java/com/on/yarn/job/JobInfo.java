package com.on.yarn.job;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.Data;

import java.io.File;

@Data
public class JobInfo {

    private String jarPath = "/mnt/dss/yarn-1.0.0.jar";

    private String appName = "api_job";

    private Long memory = 1024L;

    private String queue = "default";

    private String job;

    private String checkpoint;

    private String storePath;

    public String[] toStrinArray() {
        if (StrUtil.isBlank(checkpoint)) {
            checkpoint = "";
        }

        if (StrUtil.isBlank(storePath)) {
            checkpoint = "";
        }
        if (StrUtil.isBlank(job)) {
            throw new RuntimeException("job is null");
        }
        return new String[]{"-jar_path", jarPath, "-appname", appName, "-master_memory", memory.toString(),
                "-queue", queue, "-job", job, "-store_path", storePath, "-checkpoint", checkpoint};
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }
}
