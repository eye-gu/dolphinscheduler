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
     * ARRAY_VARCHAR,ARRAY_INTEGER,ARRAY_LONG,ARRAY_FLOAT,ARRAY_DOUBLE,ARRAY_DATE,ARRAY_TIME,ARRAY_TIMESTAMP,ARRAY_BOOLEAN
     * date, "YYYY-MM-DD"
     * time, "HH:MM:SS"
     * time stamp "YYYY-MM-DD HH:MM:SS"
     */
    private String type;

    /**
     * 来源
     * SQL_QUERY CONSTANT FUNCTION
     */
    private String from;

    /**
     * 相关配置
     */
    private String config;

    /**
     * 默认值
     */
    private String defaultValue;
    private ReadConfig readConfig;
    /**
     * 子参数, 可以配置替换config中的占位符
     */
    private List<Param> childParams;
}