/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.self;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class Column {

    /**
     * 列名
     */
    private String name;

    /**
     * 列类型
     */
    private String type;
}