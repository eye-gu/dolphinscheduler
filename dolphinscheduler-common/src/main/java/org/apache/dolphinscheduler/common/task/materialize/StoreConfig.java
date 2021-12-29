/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class StoreConfig {

    private String type;
    private String ip;
    private String port;
    private String schema;
    private String userName;
    private String password;
    private String tableName;
    private String partitionBy;
    private String distributeBy;
    private String saveMode;
    private MetaData metaData;
    private Boolean needDelete;
    private PartiotionConfig partiotionConfig;
}