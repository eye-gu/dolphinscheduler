/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.service.impl;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
@ConfigurationProperties(
    prefix = "material.light.handle"
)
@Component
public class MaterialLightHandleConfig {

    private Integer userId;
    private Long projectCode;
    private Integer tenantId;
    private Integer warningGroupId;
}