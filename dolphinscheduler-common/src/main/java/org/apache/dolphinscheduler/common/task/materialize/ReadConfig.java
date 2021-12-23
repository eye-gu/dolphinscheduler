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
public class ReadConfig {
    private String type;
    private String jdbcUrl;
    private String userName;
    private String password;
    private String path;
    private MetaData metaData;
}