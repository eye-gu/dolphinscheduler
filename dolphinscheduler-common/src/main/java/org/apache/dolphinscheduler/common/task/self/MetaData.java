/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.self;

import java.util.List;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 11:03
 */
@Data
public class MetaData {
    /**
     * 表名
     */
    private String tableName;

    /**
     * 列
     */
    private List<Column> columns;
}