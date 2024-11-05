package com.on.yarn.admin;

import cn.hutool.core.text.StrPool;
import com.on.yarn.constant.Constants;
import com.on.yarn.datafusion.DatafusionExecutor;
import com.on.yarn.arrow.Executor;
import com.on.yarn.polars.PolarsExecutor;

import java.util.Locale;

public class RoutSQL {
    public static Executor getExecutor() {
        String job = Constants.getJob();
        String type = job.split(StrPool.LF)[0].toLowerCase(Locale.ROOT).replaceAll(" ", "").replaceAll("\t", "");
        String newJob = job.substring(job.indexOf(StrPool.LF) + 1);
        if (type.startsWith("usedatafusion")) {
            return new DatafusionExecutor(newJob);
        } else if (type.startsWith("usepolars")) {
            return new PolarsExecutor(newJob);
        } else if (type.startsWith("use")) {
            throw new RuntimeException("不支持的SQL类型: " + type);
        } else {
            return new DatafusionExecutor(job);
        }
    }
}
