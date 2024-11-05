package com.on.yarn.base;

import com.on.yarn.util.YarnHelper;

/**
 * 实现操作接口
 */
public interface YarnManipulator {

    void info(String obj);

    void warn(String obj);

    void error(String obj);

    void error(String obj, Throwable t);

    /**
     * kill任务功能
     */
    default boolean killAppId(String appId) {
        return YarnHelper.killAppId(appId);
    }
}
