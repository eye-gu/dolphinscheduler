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
 */
@Data
public class Sql {
    private String sqlTemplate;
    private List<Param> params;
}