/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.asyncplatform;

import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.ResourceInfo;

import java.util.Collections;
import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class AsyncPlatformParameters extends AbstractParameters {
    @Override
    public boolean checkParameters() {
        return true;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return Collections.emptyList();
    }
}