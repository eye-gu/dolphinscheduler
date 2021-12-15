/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.asyncplatform;

import org.apache.dolphinscheduler.spi.params.base.PluginParams;
import org.apache.dolphinscheduler.spi.task.TaskChannel;
import org.apache.dolphinscheduler.spi.task.TaskChannelFactory;

import java.util.List;

import com.google.auto.service.AutoService;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 17:35
 */
@AutoService(TaskChannelFactory.class)
public class AsyncPlatformChannelFactory implements TaskChannelFactory {
    @Override
    public String getName() {
        return "ASYNC_PLATFORM";
    }

    @Override
    public List<PluginParams> getParams() {
        return null;
    }

    @Override
    public TaskChannel create() {
        return new AsyncPlatformChannel();
    }
}