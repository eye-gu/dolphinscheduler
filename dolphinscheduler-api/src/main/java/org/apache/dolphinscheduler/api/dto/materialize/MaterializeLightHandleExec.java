/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.dto.materialize;

import lombok.Data;

import java.util.Map;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-15 14:22
 */
@Data
public class MaterializeLightHandleExec {

    private String externalCode;

    private Map<String, String> startParams;
}