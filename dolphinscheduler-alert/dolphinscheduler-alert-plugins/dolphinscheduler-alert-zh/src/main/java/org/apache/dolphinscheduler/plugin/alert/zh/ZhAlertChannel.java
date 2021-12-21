/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.alert.zh;

import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;

import java.util.Map;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class ZhAlertChannel implements AlertChannel {
    @Override
    public AlertResult process(AlertInfo info) {
        Map<String, String> paramsMap = info.getAlertParams();
        if (null == paramsMap) {
            return new AlertResult("false", "http params is null");
        }
        return new ZhSender(paramsMap.get("url")).send(info.getAlertData().getContent());
    }
}