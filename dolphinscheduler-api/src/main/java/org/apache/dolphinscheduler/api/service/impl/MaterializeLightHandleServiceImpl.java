/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.service.impl;

import static org.apache.dolphinscheduler.api.enums.Status.CREATE_TASK_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.common.Constants.CMD_PARAM_START_PARAMS;

import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleExec;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleProcessDefinition;
import org.apache.dolphinscheduler.api.dto.materialize.MaterializeLightHandleTaskDefinition;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.MaterializeLightHandleService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ConditionType;
import org.apache.dolphinscheduler.common.enums.DataType;
import org.apache.dolphinscheduler.common.enums.Direct;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.enums.TimeoutFlag;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.task.materialize.MaterializeParameters;
import org.apache.dolphinscheduler.common.task.materialize.Feature;
import org.apache.dolphinscheduler.common.task.materialize.Param;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.service.process.ProcessService;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.extern.slf4j.Slf4j;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Service
@Slf4j
public class MaterializeLightHandleServiceImpl extends BaseServiceImpl implements MaterializeLightHandleService {

    @Autowired
    private ProcessService processService;

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private MaterialLightHandleConfig materialLightHandleConfig;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> create(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition) throws Exception {
        User loginUser = new User();
        loginUser.setId(materialLightHandleConfig.getUserId());

        List<TaskDefinitionLog> taskDefinitionLogs = new ArrayList<>();
        Map<String, Long> external2code = new HashMap<>(materializeLightHandleProcessDefinition.getTasks().size());
        for (MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition : materializeLightHandleProcessDefinition.getTasks()) {
            TaskDefinitionLog taskDefinitionLog = build(CodeGenerateUtils.getInstance().genCode(), 0, materializeLightHandleTaskDefinition);
            taskDefinitionLogs.add(taskDefinitionLog);

            external2code.put(materializeLightHandleTaskDefinition.getExternalCode(), taskDefinitionLog.getCode());
        }

        List<ProcessTaskRelationLog> taskRelationList = new ArrayList<>();
        for (MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition : materializeLightHandleProcessDefinition.getTasks()) {
            List<String> preExternalCodes = materializeLightHandleTaskDefinition.getPreExternalCodes();
            long postTaskCode = external2code.get(materializeLightHandleTaskDefinition.getExternalCode());
            if (CollectionUtils.isEmpty(preExternalCodes)) {
                ProcessTaskRelationLog processTaskRelationLog = build(0, 0, postTaskCode, 0);
                taskRelationList.add(processTaskRelationLog);
            } else {
                for (String preExternalCode : preExternalCodes) {
                    ProcessTaskRelationLog processTaskRelationLog = build(external2code.get(preExternalCode), 0, postTaskCode, 0);
                    taskRelationList.add(processTaskRelationLog);
                }
            }
        }

        ProcessDefinition processDefinition = build(materialLightHandleConfig.getProjectCode(),
            materialLightHandleConfig.getUserId(), materialLightHandleConfig.getTenantId(),
            CodeGenerateUtils.getInstance().genCode(), materializeLightHandleProcessDefinition);

        Map<String, Object> result = new HashMap<>();
        int saveTaskResult = processService.saveTaskDefine(loginUser, processDefinition.getProjectCode(), taskDefinitionLogs);
        if (saveTaskResult == Constants.EXIT_CODE_SUCCESS) {
            log.info("The task has not changed, so skip");
        }
        if (saveTaskResult == Constants.DEFINITION_FAILURE) {
            putMsg(result, CREATE_TASK_DEFINITION_ERROR);
            throw new ServiceException(CREATE_TASK_DEFINITION_ERROR);
        }
        int insertVersion = processService.saveProcessDefine(loginUser, processDefinition, false);
        if (insertVersion == 0) {
            putMsg(result, Status.CREATE_PROCESS_DEFINITION_ERROR);
            throw new ServiceException(Status.CREATE_PROCESS_DEFINITION_ERROR);
        }
        int insertResult = processService.saveTaskRelation(loginUser, processDefinition.getProjectCode(), processDefinition.getCode(), insertVersion, taskRelationList, taskDefinitionLogs);
        if (insertResult == Constants.EXIT_CODE_SUCCESS) {
            putMsg(result, Status.SUCCESS);
            result.put(Constants.DATA_LIST, processDefinition);
        } else {
            putMsg(result, Status.CREATE_PROCESS_TASK_RELATION_ERROR);
            throw new ServiceException(Status.CREATE_PROCESS_TASK_RELATION_ERROR);
        }
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> update(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition) throws Exception {
        User loginUser = new User();
        loginUser.setId(materialLightHandleConfig.getUserId());

        List<String> externalCodes = new ArrayList<>();
        for (MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition : materializeLightHandleProcessDefinition.getTasks()) {
            externalCodes.add(materializeLightHandleTaskDefinition.getExternalCode());
        }

        List<TaskDefinition> existTasks = taskDefinitionMapper.queryByExternalCodes(externalCodes);
        Map<String, TaskDefinition> existExternalMap = existTasks.stream().collect(Collectors.toMap(TaskDefinition::getExternalCode, Function.identity()));

        Map<String, Long> external2code = new HashMap<>(materializeLightHandleProcessDefinition.getTasks().size());
        List<TaskDefinitionLog> taskDefinitionLogs = new ArrayList<>();
        for (MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition : materializeLightHandleProcessDefinition.getTasks()) {
            TaskDefinition existTask = existExternalMap.get(materializeLightHandleTaskDefinition.getExternalCode());
            TaskDefinitionLog taskDefinitionLog = build(existTask == null ? CodeGenerateUtils.getInstance().genCode() : existTask.getCode()
                , existTask == null ? 0 : existTask.getVersion(), materializeLightHandleTaskDefinition);
            taskDefinitionLogs.add(taskDefinitionLog);

            external2code.put(materializeLightHandleTaskDefinition.getExternalCode(), taskDefinitionLog.getCode());
        }

        ProcessDefinition existProcessDefinition = processDefinitionMapper.queryByExternalCode(materializeLightHandleProcessDefinition.getExternalCode());
        ProcessDefinition processDefinition = build(materialLightHandleConfig.getProjectCode(),
            materialLightHandleConfig.getUserId(), materialLightHandleConfig.getTenantId(),
            existProcessDefinition.getCode(), materializeLightHandleProcessDefinition);
        processDefinition.setId(existProcessDefinition.getId());


        List<ProcessTaskRelationLog> taskRelationList = new ArrayList<>();
        for (MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition : materializeLightHandleProcessDefinition.getTasks()) {
            TaskDefinition existTask = existExternalMap.get(materializeLightHandleTaskDefinition.getExternalCode());
            List<String> preExternalCodes = materializeLightHandleTaskDefinition.getPreExternalCodes();
            long postTaskCode = external2code.get(materializeLightHandleTaskDefinition.getExternalCode());
            int postTaskVersion = existTask == null ? 0 : existTask.getVersion();
            if (CollectionUtils.isEmpty(preExternalCodes)) {
                ProcessTaskRelationLog processTaskRelationLog = build(0, 0,
                    postTaskCode, postTaskVersion);
                taskRelationList.add(processTaskRelationLog);
            } else {
                for (String preExternalCode : preExternalCodes) {
                    TaskDefinition pre = existExternalMap.get(preExternalCode);
                    ProcessTaskRelationLog processTaskRelationLog = build(external2code.get(preExternalCode), pre == null ? 0 : pre.getVersion(),
                        postTaskCode, postTaskVersion);
                    taskRelationList.add(processTaskRelationLog);
                }
            }
        }

        Map<String, Object> result = new HashMap<>();
        int saveTaskResult = processService.saveTaskDefine(loginUser, materialLightHandleConfig.getProjectCode(), taskDefinitionLogs);
        if (saveTaskResult == Constants.EXIT_CODE_SUCCESS) {
            log.info("The task has not changed, so skip");
        }
        if (saveTaskResult == Constants.DEFINITION_FAILURE) {
            putMsg(result, Status.UPDATE_TASK_DEFINITION_ERROR);
            throw new ServiceException(Status.UPDATE_TASK_DEFINITION_ERROR);
        }
        processDefinition.setUpdateTime(new Date());
        int insertVersion = processService.saveProcessDefine(loginUser, processDefinition, false);
        if (insertVersion == 0) {
            putMsg(result, Status.UPDATE_PROCESS_DEFINITION_ERROR);
            throw new ServiceException(Status.UPDATE_PROCESS_DEFINITION_ERROR);
        }
        int insertResult = processService.saveTaskRelation(loginUser, materialLightHandleConfig.getProjectCode(),
            processDefinition.getCode(), insertVersion, taskRelationList, taskDefinitionLogs);
        if (insertResult == Constants.EXIT_CODE_SUCCESS) {
            putMsg(result, Status.SUCCESS);
            result.put(Constants.DATA_LIST, processDefinition);
        } else {
            putMsg(result, Status.UPDATE_PROCESS_DEFINITION_ERROR);
            throw new ServiceException(Status.UPDATE_PROCESS_DEFINITION_ERROR);
        }
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> exec(MaterializeLightHandleExec materializeLightHandleExec) throws Exception {
        Map<String, Object> result = new HashMap<>();

        ProcessDefinition processDefinition = processDefinitionMapper.queryByExternalCode(materializeLightHandleExec.getExternalCode());
        if (processDefinition.getReleaseState() != ReleaseState.ONLINE) {
            // check process definition online
            putMsg(result, Status.PROCESS_DEFINE_NOT_RELEASE, materializeLightHandleExec.getExternalCode());
            return result;
        }
        Command command = new Command();

        command.setCommandType(CommandType.START_PROCESS);
        command.setProcessDefinitionCode(processDefinition.getCode());
        command.setTaskDependType(TaskDependType.TASK_POST);
        command.setFailureStrategy(FailureStrategy.END);
        command.setWarningType(WarningType.FAILURE);

        Map<String, String> cmdParam = new HashMap<>();
        if (MapUtils.isNotEmpty(materializeLightHandleExec.getStartParams())) {
            cmdParam.put(CMD_PARAM_START_PARAMS, JSONUtils.toJsonString(materializeLightHandleExec.getStartParams()));
        }
        command.setCommandParam(JSONUtils.toJsonString(cmdParam));
        command.setExecutorId(materialLightHandleConfig.getUserId());
        command.setWarningGroupId(materialLightHandleConfig.getWarningGroupId());
        command.setProcessInstancePriority(Priority.MEDIUM);
        command.setWorkerGroup(Constants.DEFAULT_WORKER_GROUP);
        command.setEnvironmentCode(-1L);
        command.setDryRun(0);
        command.setProcessDefinitionVersion(processDefinition.getVersion());
        command.setProcessInstanceId(0);
        processService.createCommand(command);
        result.put(Constants.STATUS, Status.SUCCESS);
        result.put(Constants.DATA_LIST, command);
        return result;
    }

    private ProcessDefinition build(long projectCode, int userId, int tenantId, long code,
                                    MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition) {
        List<Property> properties = Collections.emptyList();
        List<Param> params = materializeLightHandleProcessDefinition.getGlobalParams();
        if (CollectionUtils.isNotEmpty(params)) {
            properties = new ArrayList<>(params.size());
            for (Param param : params) {
                Property property = new Property();
                property.setProp(param.getName());
                property.setDirect(Direct.IN);
                property.setType(DataType.valueOf(param.getType()));
                property.setValue("");
                properties.add(property);
            }
        }
        ProcessDefinition processDefinition = new ProcessDefinition(projectCode,
            materializeLightHandleProcessDefinition.getName() + "-" + materializeLightHandleProcessDefinition.getExternalCode(),
            code,
            materializeLightHandleProcessDefinition.getDescription(),
            JSONUtils.toJsonString(properties), null,
            materializeLightHandleProcessDefinition.getTimeout(), userId, tenantId);
        processDefinition.setExternalCode(materializeLightHandleProcessDefinition.getExternalCode());
        Feature feature = new Feature();
        feature.setGlobalParams(materializeLightHandleProcessDefinition.getGlobalParams());
        processDefinition.setFeature(JSONUtils.toJsonString(feature));
        return processDefinition;
    }

    private TaskDefinitionLog build(long code, int version,
                                    MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition) {
        TaskDefinitionLog taskDefinitionLog = new TaskDefinitionLog();
        // todo
        MaterializeParameters materializeParameters = new MaterializeParameters();
        materializeParameters.setReadConfig(materializeLightHandleTaskDefinition.getReadConfig());
        materializeParameters.setStoreConfig(materializeLightHandleTaskDefinition.getStoreConfig());
        materializeParameters.setSqlLists(materializeLightHandleTaskDefinition.getSqlLists());
        materializeParameters.setLocalParams(Collections.emptyList());
        MaterialLightHandleConfig.SparkConfig sparkConfig = materialLightHandleConfig.getSpark();
        MaterializeParameters.SparkParameters sparkParameters = new MaterializeParameters.SparkParameters();
        sparkParameters.setDriverCores(sparkConfig.getDriverCores());
        sparkParameters.setDriverMemory(sparkConfig.getDriverMemory());
        sparkParameters.setExecutorMemory(sparkConfig.getExecutorMemory());
        sparkParameters.setNumExecutors(sparkConfig.getNumExecutors());
        sparkParameters.setExecutorCores(sparkConfig.getExecutorCores());
        sparkParameters.setHome(sparkConfig.getHome());
        sparkParameters.setMaster(sparkConfig.getMaster());
        sparkParameters.setMainClass(sparkConfig.getMainClass());
        sparkParameters.setMainJar(sparkConfig.getMainJar());
        sparkParameters.setDeployMode(sparkConfig.getDeployMode());
        materializeParameters.setSparkParameters(sparkParameters);
        materializeParameters.setTimeout(materializeLightHandleTaskDefinition.getTimeout());
        taskDefinitionLog.setTaskParams(JSONUtils.toJsonString(materializeParameters));
        taskDefinitionLog.setTimeout(materializeLightHandleTaskDefinition.getTimeout());
        taskDefinitionLog.setDescription(materializeLightHandleTaskDefinition.getDescription());
        taskDefinitionLog.setName(materializeLightHandleTaskDefinition.getName() + "-" + materializeLightHandleTaskDefinition.getExternalCode());
        taskDefinitionLog.setFailRetryTimes(materializeLightHandleTaskDefinition.getFailRetryTimes());
        taskDefinitionLog.setFailRetryInterval(materializeLightHandleTaskDefinition.getFailRetryInterval());
        taskDefinitionLog.setTimeoutFlag(materializeLightHandleTaskDefinition.getTimeout() > 0 ? TimeoutFlag.OPEN : TimeoutFlag.CLOSE);
        taskDefinitionLog.setDelayTime(materializeLightHandleTaskDefinition.getDelayTime());
        taskDefinitionLog.setExternalCode(materializeLightHandleTaskDefinition.getExternalCode());
        taskDefinitionLog.setFlag(Flag.YES);
        taskDefinitionLog.setCode(code);
        taskDefinitionLog.setTaskType(TaskType.MATERIALIZE.getDesc());
        taskDefinitionLog.setTaskPriority(Priority.MEDIUM);
        taskDefinitionLog.setWorkerGroup(Constants.DEFAULT_WORKER_GROUP);
        taskDefinitionLog.setTimeoutNotifyStrategy(TaskTimeoutStrategy.WARN);
        taskDefinitionLog.setEnvironmentCode(-1);
        taskDefinitionLog.setVersion(version);
        return taskDefinitionLog;
    }

    private ProcessTaskRelationLog build(long preTaskCode, int preTaskVersion,
                                         long postTaskCode, int postTaskVersion) {
        ProcessTaskRelationLog processTaskRelationLog = new ProcessTaskRelationLog();
        processTaskRelationLog.setName("");
        processTaskRelationLog.setConditionParams("{}");
        processTaskRelationLog.setConditionType(ConditionType.NONE);
        processTaskRelationLog.setPreTaskCode(preTaskCode);
        processTaskRelationLog.setPreTaskVersion(preTaskVersion);
        processTaskRelationLog.setPostTaskCode(postTaskCode);
        processTaskRelationLog.setPostTaskVersion(postTaskVersion);
        return processTaskRelationLog;
    }
}