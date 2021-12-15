/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.self;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 11:04
 */
@Data
public class Param {

    private String name;
    private String type;
    private String from;
    private String config;
    private String defaultValue;
    private ReadConfig readConfig;
}