/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.asyncplatform;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;

import org.apache.spark.launcher.SparkAppHandle;

/**
 * @author eye.gu@aloudata.com
 * @version 1
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
        asyncPlatformParameters = new AsyncPlatformParameters();
    }

    @Override
    public void init() {
        super.init();

    }

    @Override
    public void handle() throws Exception {
        super.setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
    }

    @Override
    public AbstractParameters getParameters() {
        return asyncPlatformParameters;
    }


    @Override
    public void cancelApplication(boolean status) throws Exception {
        super.cancelApplication(status);
    }
}