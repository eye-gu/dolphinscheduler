/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import org.apache.dolphinscheduler.spi.params.base.PluginParams;
import org.apache.dolphinscheduler.spi.task.TaskChannel;
import org.apache.dolphinscheduler.spi.task.TaskChannelFactory;

import java.util.List;

import com.google.auto.service.AutoService;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@AutoService(TaskChannelFactory.class)
public class MaterializeChannelFactory implements TaskChannelFactory {
    @Override
    public String getName() {
        return "MATERIALIZE";
    }

    @Override
    public List<PluginParams> getParams() {
        return null;
    }

    @Override
    public TaskChannel create() {
        return new MaterializeChannel();
    }
}