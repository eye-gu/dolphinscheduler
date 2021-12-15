/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleExec;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleProcessDefinition;

import java.util.Map;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 16:32
 */
public interface MaterializeLightHandleService {

    Map<String, Object> create(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition) throws Exception;

    Map<String, Object> update(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition) throws Exception;

    Map<String, Object> exec(MaterializeLightHandleExec materializeLightHandleExec) throws Exception;
}