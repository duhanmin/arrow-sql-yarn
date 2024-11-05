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

    //@ApiModelProperty(name = "jarPath", value = "远程执行jar", dataType = "string", example = "s3://canary-lb-bi-presto-config/yarn-1.0.0.jar", required = true)
    private String jarPath = "/mnt/dss/yarn-1.0.0.jar";

    //@ApiModelProperty(name = "appName", value = "任务名", dataType = "string", example = "api_job")
    private String appName = "api_job";

    //@ApiModelProperty(name = "memory", value = "执行内存", dataType = "long", example = "1024")
    private Long memory = 1024L;

    //@ApiModelProperty(name = "queue", value = "队列", dataType = "string", example = "default")
    private String queue = "default";

    //@ApiModelProperty(name = "job", value = "任务json", dataType = "object", example = "{}", required = true)
    private Object job;

    //@ApiModelProperty(name = "job", value = "任务json", dataType = "object", example = "{}", required = true)
    private File jobPath;

    private String checkpoint;

    public String[] toStrinArray() {
        if (ObjectUtil.isNull(jobPath) && ObjectUtil.isNull(job)) {
            throw new IllegalArgumentException("jobPath or job is null");
        }
        if (ObjectUtil.isNull(jobPath) && ObjectUtil.isNotNull(job)) {
            File mkdir = FileUtil.mkdir("/tmp/job-api/job/");
            jobPath = FileUtil.touch(mkdir.getAbsolutePath() + "/" + UUID.fastUUID().toString(true) + ".json");
            FileUtil.writeUtf8String(JSONUtil.toJsonStr(job), jobPath);
        }
        if (StrUtil.isBlank(checkpoint)) {
            checkpoint = "";
        }
        return new String[]{"-jar_path", jarPath, "-appname", appName, "-master_memory", memory.toString(),
                "-queue", queue, "-job", jobPath.getAbsolutePath(), "-checkpoint", checkpoint};
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }
}
