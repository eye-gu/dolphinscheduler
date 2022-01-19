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

    /**
     * @see ReadOrStoreConfigTypeEnum
     */
    private String type;
    private String ip;
    private String port;
    private String schema;
    /**
     * 数据库
     */
    private String database;
    private String userName;
    private String password;
    private String tableName;
    private String partitionBy;
    private String distributeBy;
    private String saveMode;
    private MetaData metaData;
    private Boolean needDelete;
    private PartitionConfig partitionConfig;
    /**
     * 存储文件类型
     * excel csv dat
     */
    private String fileType;
}