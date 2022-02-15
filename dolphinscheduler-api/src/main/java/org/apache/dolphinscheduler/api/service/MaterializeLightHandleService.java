/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleExec;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleProcessDefinition;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.web.multipart.MultipartFile;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public interface MaterializeLightHandleService {

    Map<String, Object> create(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition, MultipartFile[] files) throws Exception;

    Map<String, Object> update(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition, MultipartFile[] files) throws Exception;

    Map<String, Object> exec(MaterializeLightHandleExec materializeLightHandleExec) throws Exception;

    Map<String, Object> status(Integer commandId);

    Map<String, Object> statuses(Set<Integer> commandIds);
}