/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.asyncplatform;

import org.apache.dolphinscheduler.spi.task.AbstractTask;
import org.apache.dolphinscheduler.spi.task.TaskChannel;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class AsyncPlatformChannel implements TaskChannel {
    @Override
    public void cancelApplication(boolean status) {

    }

    @Override
    public AbstractTask createTask(TaskRequest taskRequest) {
        return new AsyncPlatformTask(taskRequest);
    }
}