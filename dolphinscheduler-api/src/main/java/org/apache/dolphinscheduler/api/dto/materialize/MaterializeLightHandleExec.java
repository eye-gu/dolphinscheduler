/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.dto.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.StoreConfig;

import java.util.List;
import java.util.Map;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class MaterializeLightHandleExec {

    private String externalCode;

    private Map<String, String> startParams;

    private List<ReadConfig> readConfigs;

    private StoreConfig resultStoreConfig;

    /**
     * 是否空跑
     */
    private Boolean dryRun;
}