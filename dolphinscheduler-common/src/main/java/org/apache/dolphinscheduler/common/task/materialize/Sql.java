/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.materialize;

import java.util.List;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class Sql {
    /**
     * sql模板
     */
    private String sqlTemplate;
    /**
     * 该sql的参数,为kylin的in准备
     */
    private List<Param> params;
}