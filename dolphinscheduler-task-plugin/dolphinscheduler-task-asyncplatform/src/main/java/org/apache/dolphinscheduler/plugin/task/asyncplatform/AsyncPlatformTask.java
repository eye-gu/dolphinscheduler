/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.asyncplatform;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 17:36
 */
public class AsyncPlatformTask extends AbstractTaskExecutor {

    private AsyncPlatformParameters asyncPlatformParameters;
    /**
     * constructor
     *
     * @param taskRequest taskRequest
     */
    protected AsyncPlatformTask(TaskRequest taskRequest) {
        super(taskRequest);
    }

    @Override
    public void init() {
        super.init();
        asyncPlatformParameters = new AsyncPlatformParameters();
    }

    @Override
    public void handle() throws Exception {
        super.setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
    }

    @Override
    public AbstractParameters getParameters() {
        return asyncPlatformParameters;
    }
}