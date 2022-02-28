/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.controller;

import static org.apache.dolphinscheduler.api.enums.Status.COUNT_PROCESS_INSTANCE_STATE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CREATE_TASK_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.START_PROCESS_INSTANCE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.UPDATE_TASK_DEFINITION_ERROR;

import org.apache.dolphinscheduler.api.aspect.AccessLogAnnotation;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleExec;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleProcessDefinition;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleTaskDefinition;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.MaterializeLightHandleService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.task.materialize.JobRunInfo;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import springfox.documentation.annotations.ApiIgnore;

import com.baomidou.mybatisplus.annotation.TableField;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Api(tags = "MATERIALIZE_LIGHT_HANDLE")
@RestController
@RequestMapping("/materialize_light_handle")
@Slf4j
public class MaterializeLightHandleController extends BaseController {

    private static final String command_prefix = "materialize";

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
    public Result create(@RequestParam(value = "externalCode") String externalCode,
                         @RequestParam("materializeLightHandleProcessDefinition") MultipartFile materializeLightHandleProcessDefinition,
                         @RequestParam(value = "files", required = false) MultipartFile[] files) throws Exception {
        MaterializeLightHandleProcessDefinition processDefinition = getFromFile(materializeLightHandleProcessDefinition);
        processDefinition.setExternalCode(externalCode);
        if (invalid(processDefinition)) {
            throw new IllegalArgumentException("processDefinition is invalid");
        }
        return returnDataList(materializeLightHandleService.create(processDefinition, files));
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
    public Result update(@RequestParam(value = "externalCode") String externalCode,
                         @RequestParam("materializeLightHandleProcessDefinition") MultipartFile materializeLightHandleProcessDefinition,
                         @RequestParam(value = "files", required = false) MultipartFile[] files) throws Exception {
        MaterializeLightHandleProcessDefinition processDefinition = getFromFile(materializeLightHandleProcessDefinition);
        processDefinition.setExternalCode(externalCode);
        if (invalid(processDefinition)) {
            throw new IllegalArgumentException("processDefinition is invalid");
        }
        return returnDataList(materializeLightHandleService.update(processDefinition, files));
    }


    @ApiOperation(value = "exec", notes = "EXEC")
    @PostMapping(value = "/exec")
    @ApiException(START_PROCESS_INSTANCE_ERROR)
    @AccessLogAnnotation
    public Result exec(@RequestBody MaterializeLightHandleExec materializeLightHandleExec) throws Exception {
        if (materializeLightHandleExec == null || StringUtils.isBlank(materializeLightHandleExec.getExternalCode())) {
            throw new IllegalArgumentException("exec process is invalid");
        }
        Map<String, Object> result = materializeLightHandleService.exec(materializeLightHandleExec);
        Command command = (Command) result.get(Constants.DATA_LIST);
        if (command != null) {
            result.put(Constants.DATA_LIST, convert(command));
        }
        return returnDataList(result);
    }

    @ApiOperation(value = "status", notes = "STATUS")
    @GetMapping(value = "/status")
    @ApiException(COUNT_PROCESS_INSTANCE_STATE_ERROR)
    @AccessLogAnnotation
    public Result status(@RequestParam String commandId) throws Exception {
        if (commandId == null) {
            throw new IllegalArgumentException("commandId is invalid");
        }
        Map<String, Object> result = materializeLightHandleService.status(parsePrefix(commandId));
        JobRunInfo jobRunInfo = (JobRunInfo) result.get(Constants.DATA_LIST);
        if (jobRunInfo != null) {
            result.put(Constants.DATA_LIST, convert(jobRunInfo));
        }
        return returnDataList(result);
    }

    @ApiOperation(value = "statuses", notes = "STATUSES")
    @GetMapping(value = "/statuses")
    @ApiException(COUNT_PROCESS_INSTANCE_STATE_ERROR)
    @AccessLogAnnotation
    public Result statuses(@RequestParam List<String> commandIds) throws Exception {
        if (CollectionUtils.isEmpty(commandIds) || commandIds.size() > 100) {
            throw new IllegalArgumentException("commandIds size must be between 1 and 100");
        }
        Map<String, Object> result = materializeLightHandleService.statuses(commandIds.stream().map(this::parsePrefix).collect(Collectors.toSet()));
        List<JobRunInfo> jobRunInfos = (List<JobRunInfo>) result.get(Constants.DATA_LIST);
        if (CollectionUtils.isNotEmpty(jobRunInfos)) {
            result.put(Constants.DATA_LIST, jobRunInfos.stream().map(this::convert).collect(Collectors.toList()));
        }
        return returnDataList(result);
    }


    private JobRunInfo convert(JobRunInfo jobRunInfo) {
        jobRunInfo.setJobId(command_prefix + jobRunInfo.getJobId());
        return jobRunInfo;
    }

    private ResultCommand convert(Command command) {
        ResultCommand resultCommand = new ResultCommand();
        resultCommand.setId(command_prefix + command.getId());
        resultCommand.setCommandType(resultCommand.getCommandType());
        resultCommand.setProcessDefinitionCode(resultCommand.getProcessDefinitionVersion());
        resultCommand.setCommandParam(resultCommand.getCommandParam());
        resultCommand.setProcessInstanceId(resultCommand.getProcessInstanceId());
        resultCommand.setProcessDefinitionVersion(resultCommand.getProcessDefinitionVersion());
        return resultCommand;
    }

    private Integer parsePrefix(String commandId) {
        if (commandId.startsWith(command_prefix)) {
            commandId = commandId.substring(command_prefix.length());
        }
        return Integer.valueOf(commandId);
    }

    private MaterializeLightHandleProcessDefinition getFromFile(MultipartFile file) {
        try {
            String json = IOUtils.toString(file.getInputStream());
            return JSONUtils.parseObject(json, MaterializeLightHandleProcessDefinition.class);
        } catch (IOException e) {
            log.error("parse process definition file error", e);
            throw new IllegalArgumentException("parse process definition file error");
        }
    }


    private boolean invalid(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition) {
        if (materializeLightHandleProcessDefinition == null) {
            log.error("process is null");
            return true;
        }
        if (CollectionUtils.isEmpty(materializeLightHandleProcessDefinition.getTasks())) {
            log.error("process tasks is null");
            return true;
        }
        for (MaterializeLightHandleTaskDefinition task : materializeLightHandleProcessDefinition.getTasks()) {
            if (task == null) {
                log.error("task is null");
                return true;
            }
            if (StringUtils.isBlank(task.getExternalCode())) {
                log.error("task external code is null");
                return true;
            }
            if (CollectionUtils.isEmpty(task.getSqlList())) {
                log.error("task sql list is null");
                return true;
            }
            task.setExternalCode(materializeLightHandleProcessDefinition.getExternalCode() + "-" + task.getExternalCode());
            if (CollectionUtils.isNotEmpty(task.getPreExternalCodes())) {
                List<String> preCodes = new ArrayList<>(task.getPreExternalCodes().size());
                for (String preExternalCode : task.getPreExternalCodes()) {
                    preCodes.add(materializeLightHandleProcessDefinition.getExternalCode() + "-" + preExternalCode);
                }
                task.setPreExternalCodes(preCodes);
            }
        }
        return false;
    }

    @Data
    private static class ResultCommand {
        private String id;

        /**
         * command type
         */
        private CommandType commandType;

        /**
         * process definition code
         */
        private long processDefinitionCode;


        /**
         * command parameter, format json
         */
        private String commandParam;

        private int processInstanceId;

        private int processDefinitionVersion;
    }
}