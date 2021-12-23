/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class MaterializeTask extends AbstractTaskExecutor {

    private MaterializeParameters materializeParameters;
    /**
     * constructor
     *
     * @param taskRequest taskRequest
     */
    protected MaterializeTask(TaskRequest taskRequest) {
        super(taskRequest);
        materializeParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), MaterializeParameters.class);
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
        return materializeParameters;
    }


    @Override
    public void cancelApplication(boolean status) throws Exception {
        super.cancelApplication(status);
    }
}