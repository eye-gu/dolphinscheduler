/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.alert.zh;

import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertChannelFactory;
import org.apache.dolphinscheduler.spi.params.base.FormType;
import org.apache.dolphinscheduler.spi.params.base.PluginParams;

import java.util.Collections;
import java.util.List;

import com.google.auto.service.AutoService;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-15 20:57
 */
@AutoService(AlertChannelFactory.class)
public class ZhAlertChannelFactory implements AlertChannelFactory {
    @Override
    public String name() {
        return "zh";
    }

    @Override
    public AlertChannel create() {
        return new ZhAlertChannel();
    }

    @Override
    public List<PluginParams> params() {
        PluginParams.Builder builder = new PluginParams.Builder("url", FormType.INPUT, "url");
        return Collections.singletonList(builder.build());
    }
}