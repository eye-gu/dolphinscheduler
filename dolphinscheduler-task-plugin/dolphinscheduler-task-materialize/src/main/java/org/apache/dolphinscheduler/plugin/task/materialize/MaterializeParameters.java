/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.Sql;
import org.apache.dolphinscheduler.common.task.materialize.StoreConfig;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.ResourceInfo;

import java.util.Collections;
import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class MaterializeParameters extends AbstractParameters {
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