/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.controller;

import static org.apache.dolphinscheduler.api.enums.Status.COUNT_PROCESS_INSTANCE_STATE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CREATE_TASK_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.START_PROCESS_INSTANCE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.UPDATE_TASK_DEFINITION_ERROR;

import org.apache.dolphinscheduler.api.aspect.AccessLogAnnotation;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleExec;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleProcessDefinition;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.MaterializeLightHandleService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import springfox.documentation.annotations.ApiIgnore;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Api(tags = "MATERIALIZE_LIGHT_HANDLE")
@RestController
@RequestMapping("/materialize_light_handle")
@Slf4j
public class MaterializeLightHandleController extends BaseController {

    @Autowired
    private MaterializeLightHandleService materializeLightHandleService;

    @ApiOperation(value = "create", notes = "CREATE")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "externalCode", value = "EXTERNAL_CODE", required = true, dataType = "String"),
        @ApiImplicitParam(name = "globalParams", value = "EXTERNAL_CODE", required = true, dataType = "List"),
        @ApiImplicitParam(name = "globalParams[*].name", value = "EXTERNAL_CODE", required = true, dataType = "String"),
    })
    @PostMapping(value = "/create")
    @ApiException(CREATE_TASK_DEFINITION_ERROR)
    @AccessLogAnnotation
    public Result create(@RequestParam("materializeLightHandleProcessDefinition") @ApiIgnore String materializeLightHandleProcessDefinition,
                         @RequestParam(value = "files", required = false) MultipartFile[] files) throws Exception {
        return returnDataList(materializeLightHandleService.create(JSONUtils.parseObject(materializeLightHandleProcessDefinition, MaterializeLightHandleProcessDefinition.class), files));
    }

    @ApiOperation(value = "update", notes = "UPDATE")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "externalCode", value = "EXTERNAL_CODE", required = true, dataType = "String"),
        @ApiImplicitParam(name = "globalParams", value = "EXTERNAL_CODE", required = true, dataType = "List"),
        @ApiImplicitParam(name = "globalParams[*].name", value = "EXTERNAL_CODE", required = true, dataType = "String"),
    })
    @PostMapping(value = "/update")
    @ApiException(UPDATE_TASK_DEFINITION_ERROR)
    @AccessLogAnnotation
    public Result update(@RequestParam("materializeLightHandleProcessDefinition") @ApiIgnore String materializeLightHandleProcessDefinition,
                         @RequestParam(value = "files", required = false) MultipartFile[] files) throws Exception {
        return returnDataList(materializeLightHandleService.update(JSONUtils.parseObject(materializeLightHandleProcessDefinition, MaterializeLightHandleProcessDefinition.class), files));
    }


    @ApiOperation(value = "exec", notes = "EXEC")
    @PostMapping(value = "/exec")
    @ApiException(START_PROCESS_INSTANCE_ERROR)
    @AccessLogAnnotation
    public Result exec(@RequestBody MaterializeLightHandleExec materializeLightHandleExec) throws Exception {
        return returnDataList(materializeLightHandleService.exec(materializeLightHandleExec));
    }

    @ApiOperation(value = "status", notes = "STATUS")
    @GetMapping(value = "/status")
    @ApiException(COUNT_PROCESS_INSTANCE_STATE_ERROR)
    @AccessLogAnnotation
    public Result status(@RequestParam Integer commandId) throws Exception {
        return returnDataList(materializeLightHandleService.status(commandId));
    }
}