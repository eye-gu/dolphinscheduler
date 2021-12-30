/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class Param {

    /**
     * 参数名
     */
    private String name;

    /**
     * 类型
     * VARCHAR,INTEGER,LONG,FLOAT,DOUBLE,DATE,TIME,TIMESTAMP,BOOLEAN
     * date, "YYYY-MM-DD"
     * time, "HH:MM:SS"
     * time stamp "YYYY-MM-DD HH:MM:SS"
     */
    private String type;

    /**
     * 是否列表
     */
    private Boolean array;

    private List<ParamValue> paramValues;
}