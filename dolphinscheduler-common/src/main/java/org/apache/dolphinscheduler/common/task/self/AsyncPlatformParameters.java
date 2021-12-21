/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.self;

import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;

import java.util.Collections;
import java.util.List;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class AsyncPlatformParameters extends AbstractParameters {

    private ReadConfig readConfig;
    private StoreConfig storeConfig;
    private List<Sql> sqlLists;

    @Override
    public boolean checkParameters() {
        return true;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return Collections.emptyList();
    }
}