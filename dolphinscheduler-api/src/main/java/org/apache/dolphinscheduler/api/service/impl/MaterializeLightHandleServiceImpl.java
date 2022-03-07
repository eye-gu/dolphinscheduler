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
import org.apache.dolphinscheduler.common.enums.Direct;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
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
import org.apache.dolphinscheduler.common.task.materialize.JSONUtils;
import org.apache.dolphinscheduler.common.task.materialize.JobRunInfo;
import org.apache.dolphinscheduler.common.task.materialize.JobStatus;
import org.apache.dolphinscheduler.common.task.materialize.MaterializeParameters;
import org.apache.dolphinscheduler.common.task.materialize.Feature;
import org.apache.dolphinscheduler.common.task.materialize.Param;
import org.apache.dolphinscheduler.common.task.materialize.ParamUtils;
import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.ReadOrStoreConfigTypeEnum;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.common.utils.HadoopUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.CommandProcessInstanceRelation;
import org.apache.dolphinscheduler.dao.entity.ErrorCommand;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.CommandMapper;
import org.apache.dolphinscheduler.dao.mapper.ErrorCommandMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessInstanceMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessTaskRelationLogMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionLogMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.apache.dolphinscheduler.remote.command.StateEventChangeCommand;
import org.apache.dolphinscheduler.remote.processor.StateEventCallbackService;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import io.swagger.models.auth.In;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Lists;

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

    @Autowired
    private TaskInstanceMapper taskInstanceMapper;

    @Autowired
    private ProcessTaskRelationLogMapper processTaskRelationLogMapper;

    @Autowired
    private ErrorCommandMapper errorCommandMapper;

    @Autowired
    private CommandMapper commandMapper;

    @Autowired
    private ProcessInstanceMapper processInstanceMapper;

    @Autowired
    private TaskDefinitionLogMapper taskDefinitionLogMapper;

    @Autowired
    private StateEventCallbackService stateEventCallbackService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> create(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition,
                                      MultipartFile[] files) throws Exception {

        ProcessDefinition existProcessDefinition = processDefinitionMapper.queryByExternalCode(materializeLightHandleProcessDefinition.getExternalCode());
        if (existProcessDefinition != null) {
            return this.update(materializeLightHandleProcessDefinition, files);
        }

        User loginUser = new User();
        loginUser.setId(materialLightHandleConfig.getUserId());

        List<TaskDefinitionLog> taskDefinitionLogs = new ArrayList<>();
        Map<String, Long> external2code = new HashMap<>(materializeLightHandleProcessDefinition.getTasks().size());
        for (MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition : materializeLightHandleProcessDefinition.getTasks()) {
            TaskDefinitionLog taskDefinitionLog = build(materializeLightHandleProcessDefinition.getExternalCode(),
                CodeGenerateUtils.getInstance().genCode(), 0, materializeLightHandleTaskDefinition);
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

        uploadToHdfs(materializeLightHandleProcessDefinition.getExternalCode(), files);
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> update(MaterializeLightHandleProcessDefinition materializeLightHandleProcessDefinition,
                                      MultipartFile[] files) throws Exception {
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
            TaskDefinitionLog taskDefinitionLog = build(materializeLightHandleProcessDefinition.getExternalCode(),
                existTask == null ? CodeGenerateUtils.getInstance().genCode() : existTask.getCode()
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

        HadoopUtils.getInstance().delete("/tmp/material_light_handle/file/" + materializeLightHandleProcessDefinition.getExternalCode(), true);
        uploadToHdfs(materializeLightHandleProcessDefinition.getExternalCode(), files);
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> exec(MaterializeLightHandleExec materializeLightHandleExec) throws Exception {
        Map<String, Object> result = new HashMap<>(2);

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

        Map<String, String> cmdParam = new HashMap<>(1);
        Map<String, String> startParams = materializeLightHandleExec.getStartParams();
        if (startParams == null) {
            startParams = new HashMap<>();
        }
        startParams.put(ParamUtils.RESULT_STORE_CONFIG, JSONUtils.toJsonString(materializeLightHandleExec.getResultStoreConfig()));
        if (CollectionUtils.isNotEmpty(materializeLightHandleExec.getReadConfigs())) {
            for (ReadConfig readConfig : materializeLightHandleExec.getReadConfigs()) {
                startParams.put(ParamUtils.READ_CONFIG + readConfig.getDatasourceId(), JSONUtils.toJsonString(readConfig));
            }
        }
        if (Objects.nonNull(materializeLightHandleExec.getDryRun()) && materializeLightHandleExec.getDryRun()) {
            startParams.put(ParamUtils.DRY_RUN, Boolean.TRUE.toString());
        }
        cmdParam.put(CMD_PARAM_START_PARAMS, JSONUtils.toJsonString(startParams));
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

    @Override
    public Map<String, Object> status(Integer commandId) {
        Map<String, Object> result = new HashMap<>(2);


        Integer processInstanceId = processService.queryProcessInstanceByCommandId(commandId);
        if (processInstanceId == null) {
            ErrorCommand errorCommand = errorCommandMapper.selectById(commandId);
            if (errorCommand != null) {
                ProcessDefinition processDefinition = processDefinitionMapper.queryByCode(errorCommand.getProcessDefinitionCode());
                JobRunInfo jobRunInfo = build(commandId, errorCommand, processDefinition);
                result.put(Constants.STATUS, Status.SUCCESS);
                result.put(Constants.DATA_LIST, jobRunInfo);
                return result;
            } else {
                Command command = commandMapper.selectById(commandId);
                if (command != null) {
                    JobRunInfo jobRunInfo = build(commandId, command);
                    result.put(Constants.STATUS, Status.SUCCESS);
                    result.put(Constants.DATA_LIST, jobRunInfo);
                    return result;
                } else {
                    JobRunInfo jobRunInfo = build(commandId);
                    result.put(Constants.STATUS, Status.SUCCESS);
                    result.put(Constants.DATA_LIST, jobRunInfo);
                    return result;
                }
            }
        }
        List<TaskInstance> taskInstances = taskInstanceMapper.findTaskListByProcessId(processInstanceId);
        ProcessInstance processInstance = processService.findProcessInstanceDetailById(processInstanceId);
        JobRunInfo jobRunInfo = build(processInstance, commandId, taskInstances);
        result.put(Constants.STATUS, Status.SUCCESS);
        result.put(Constants.DATA_LIST, jobRunInfo);
        return result;
    }

    @Override
    public Map<String, Object> statuses(Set<Integer> commandIds) {
        Map<String, Object> result = new HashMap<>(2);


        List<JobRunInfo> jobRunInfos = new ArrayList<>(commandIds.size());
        List<CommandProcessInstanceRelation> commandProcessInstanceRelations = processService.queryByCommandIds(commandIds);

        if (CollectionUtils.isNotEmpty(commandProcessInstanceRelations)) {
            Map<Integer, Integer> commandIdProcessInstanceIdMap = commandProcessInstanceRelations.stream()
                .collect(Collectors.toMap(CommandProcessInstanceRelation::getCommandId, CommandProcessInstanceRelation::getProcessInstanceId));
            Collection<Integer> processInstanceIds = commandIdProcessInstanceIdMap.values();
            List<ProcessInstance> processInstances = processInstanceMapper.selectBatchIds(processInstanceIds);
            Map<Integer, ProcessInstance> processInstanceMap = processInstances.stream()
                .collect(Collectors.toMap(ProcessInstance::getId, Function.identity()));
            List<TaskInstance> taskInstances = taskInstanceMapper.findTaskListByProcessIds(processInstanceIds);
            Map<Integer, List<TaskInstance>> processInstanceTaskMap = taskInstances.stream()
                    .collect(Collectors.groupingBy(TaskInstance::getProcessInstanceId));
            for (Map.Entry<Integer, Integer> entry : commandIdProcessInstanceIdMap.entrySet()) {
                Integer commandId = entry.getKey();
                commandIds.remove(commandId);
                jobRunInfos.add(build(processInstanceMap.get(entry.getValue()), commandId, processInstanceTaskMap.get(entry.getValue())));
            }
        }

        if (commandIds.size() > 0) {
            List<ErrorCommand> errorCommands = errorCommandMapper.selectBatchIds(commandIds);
            for (ErrorCommand errorCommand : errorCommands) {
                commandIds.remove(errorCommand.getId());
                jobRunInfos.add(build(errorCommand.getId(), errorCommand, null));
            }
        }

        if (commandIds.size() > 0) {
            List<Command> commands = commandMapper.selectBatchIds(commandIds);
            for (Command command : commands) {
                commandIds.remove(command.getId());
                jobRunInfos.add(build(command.getId(), command));
            }
        }

        if (commandIds.size() > 0) {
            for (Integer commandId : commandIds) {
                jobRunInfos.add(build(commandId));
            }
        }

        result.put(Constants.STATUS, Status.SUCCESS);
        result.put(Constants.DATA_LIST, jobRunInfos);
        return result;
    }

    @Override
    public Map<String, Object> stop(Integer commandId) {
        Map<String, Object> result = new HashMap<>(2);
        Integer processInstanceId = processService.queryProcessInstanceByCommandId(commandId);
        if (processInstanceId == null) {
            putMsg(result, Status.PROCESS_INSTANCE_NOT_EXIST, commandId);
            return result;
        }
        ProcessInstance processInstance = processService.findProcessInstanceDetailById(processInstanceId);
        if (processInstance == null) {
            putMsg(result, Status.PROCESS_INSTANCE_NOT_EXIST, processInstanceId);
            return result;
        }
        if (processInstance.getState() == ExecutionStatus.READY_STOP) {
            putMsg(result, Status.PROCESS_INSTANCE_ALREADY_CHANGED, processInstance.getName(), processInstance.getState());
        } else {
            result = updateProcessInstancePrepare(processInstance, CommandType.STOP, ExecutionStatus.READY_STOP);
        }
        return result;
    }

    private Map<String, Object> updateProcessInstancePrepare(ProcessInstance processInstance, CommandType commandType, ExecutionStatus executionStatus) {
        Map<String, Object> result = new HashMap<>();

        processInstance.setCommandType(commandType);
        processInstance.addHistoryCmd(commandType);
        processInstance.setState(executionStatus);
        int update = processService.updateProcessInstance(processInstance);

        // determine whether the process is normal
        if (update > 0) {
            String host = processInstance.getHost();
            String address = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            StateEventChangeCommand stateEventChangeCommand = new StateEventChangeCommand(
                processInstance.getId(), 0, processInstance.getState(), processInstance.getId(), 0
            );
            stateEventCallbackService.sendResult(address, port, stateEventChangeCommand.convert2Command());
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.EXECUTE_PROCESS_INSTANCE_ERROR);
        }
        return result;
    }

    private int taskSize(long processCode, int processVersion) {
        List<ProcessTaskRelationLog> processTaskRelationLogs = processTaskRelationLogMapper.queryByProcessCodeAndVersion(processCode, processVersion);
        if (CollectionUtils.isEmpty(processTaskRelationLogs)) {
            return 0;
        }
        Set<Long> taskCodes = new HashSet<>();
        for (ProcessTaskRelationLog processTaskRelationLog : processTaskRelationLogs) {
            if (processTaskRelationLog.getPreTaskCode() != 0) {
                taskCodes.add(processTaskRelationLog.getPreTaskCode());
            }
            if (processTaskRelationLog.getPostTaskCode() != 0) {
                taskCodes.add(processTaskRelationLog.getPostTaskCode());
            }
        }
        return taskCodes.size();
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
                property.setType(ParamUtils.convertToDataType(param));
                property.setValue("");
                properties.add(property);
            }
        }
        String name;
        if (StringUtils.isBlank(materializeLightHandleProcessDefinition.getName())) {
            name = materializeLightHandleProcessDefinition.getExternalCode();
        } else if (materializeLightHandleProcessDefinition.getName().contains(materializeLightHandleProcessDefinition.getExternalCode())) {
            name = materializeLightHandleProcessDefinition.getName();
        } else {
            name = materializeLightHandleProcessDefinition.getName() + "-" + materializeLightHandleProcessDefinition.getExternalCode();
        }
        ProcessDefinition processDefinition = new ProcessDefinition(projectCode,
            name,
            code,
            materializeLightHandleProcessDefinition.getDescription(),
            JSONUtils.toJsonString(properties), null,
            primaryIntGet(materializeLightHandleProcessDefinition::getTimeout), userId, tenantId);
        processDefinition.setExternalCode(materializeLightHandleProcessDefinition.getExternalCode());
        Feature feature = new Feature();
        feature.setGlobalParams(materializeLightHandleProcessDefinition.getGlobalParams());
        processDefinition.setFeature(JSONUtils.toJsonString(feature));
        return processDefinition;
    }

    private TaskDefinitionLog build(String processExternalCode, long code, int version,
                                    MaterializeLightHandleTaskDefinition materializeLightHandleTaskDefinition) {
        TaskDefinitionLog taskDefinitionLog = new TaskDefinitionLog();
        MaterializeParameters materializeParameters = new MaterializeParameters();
        ReadConfig readConfig = materializeLightHandleTaskDefinition.getReadConfig();
        if (readConfig != null) {
            if (ReadOrStoreConfigTypeEnum.FILE.name().equalsIgnoreCase(readConfig.getType())) {
                if (StringUtils.isNotBlank(readConfig.getFileType())) {
                    readConfig.setType(readConfig.getFileType());
                } else {
                    readConfig.setType(ReadOrStoreConfigTypeEnum.EXCEL.name());
                }
                readConfig.setPath("/tmp/material_light_handle/file/" + processExternalCode + "/" + readConfig.getDatasourceId());
            }
        }
        materializeParameters.setReadConfig(readConfig);
        materializeParameters.setStoreConfig(materializeLightHandleTaskDefinition.getStoreConfig());
        materializeParameters.setSqlList(materializeLightHandleTaskDefinition.getSqlList());
        materializeParameters.setLocalParams(Collections.emptyList());
        materializeParameters.setTimeout(primaryIntGet(materializeLightHandleTaskDefinition::getTimeout));
        materializeParameters.setExternalCode(materializeLightHandleTaskDefinition.getExternalCode());
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
        taskDefinitionLog.setTaskParams(JSONUtils.toJsonString(materializeParameters));
        taskDefinitionLog.setTimeout(primaryIntGet(materializeLightHandleTaskDefinition::getTimeout));
        taskDefinitionLog.setDescription(materializeLightHandleTaskDefinition.getDescription());
        if (StringUtils.isBlank(materializeLightHandleTaskDefinition.getName())) {
            taskDefinitionLog.setName(materializeLightHandleTaskDefinition.getExternalCode());
        } else if (materializeLightHandleTaskDefinition.getName().contains(materializeLightHandleTaskDefinition.getExternalCode())) {
            taskDefinitionLog.setName(materializeLightHandleTaskDefinition.getName());
        } else {
            taskDefinitionLog.setName(materializeLightHandleTaskDefinition.getName() + "-" + materializeLightHandleTaskDefinition.getExternalCode());
        }
        taskDefinitionLog.setFailRetryTimes(primaryIntGet(materializeLightHandleTaskDefinition::getFailRetryTimes));
        taskDefinitionLog.setFailRetryInterval(primaryIntGet(materializeLightHandleTaskDefinition::getFailRetryInterval));
        taskDefinitionLog.setTimeoutFlag(taskDefinitionLog.getTimeout() > 0 ? TimeoutFlag.OPEN : TimeoutFlag.CLOSE);
        taskDefinitionLog.setDelayTime(primaryIntGet(materializeLightHandleTaskDefinition::getDelayTime));
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

    private JobRunInfo build(ProcessInstance processInstance, Integer commandId, List<TaskInstance> taskInstances) {
        JobRunInfo jobRunInfo = new JobRunInfo();
        jobRunInfo.setJobId(String.valueOf(commandId));
        if (processInstance == null) {
            jobRunInfo.setJobStatus(JobStatus.FAILED);
            jobRunInfo.setErrorMsg("未找到该任务");
            jobRunInfo.setJobCompleteRate(String.format(JobRunInfo.JOB_COMPLETE_RATE_FORMAT, 0, 0));
            return jobRunInfo;
        }
        ExecutionStatus status = processInstance.getState();
        switch (status) {
            case READY_PAUSE:
            case PAUSE:
            case READY_STOP:
            case STOP:
            case KILL:
                jobRunInfo.setJobStatus(JobStatus.STOP);
                break;
            case SUCCESS:
            case FORCED_SUCCESS:
                jobRunInfo.setJobStatus(JobStatus.SUCCESS);
                break;
            case FAILURE:
                jobRunInfo.setJobStatus(JobStatus.FAILED);
                break;
            default:
                jobRunInfo.setJobStatus(JobStatus.RUNNING);
                break;
        }

        int successSize = taskInstances.stream().filter(t -> t.getState().equals(ExecutionStatus.SUCCESS) || t.getState().equals(ExecutionStatus.FORCED_SUCCESS))
            .collect(Collectors.groupingBy(TaskInstance::getTaskCode)).size();
        jobRunInfo.setJobCompleteRate(String.format(JobRunInfo.JOB_COMPLETE_RATE_FORMAT, successSize, taskSize(processInstance.getProcessDefinitionCode(), processInstance.getProcessDefinitionVersion())));
        if (jobRunInfo.getJobStatus().equals(JobStatus.FAILED)) {
            Optional<TaskInstance> fail = taskInstances.stream().filter(t -> t.getState().equals(ExecutionStatus.FAILURE)).findFirst();
            if (fail.isPresent()) {
                jobRunInfo.setErrorMsg(fail.get().getName() + "失败");
            } else {
                jobRunInfo.setErrorMsg("未知异常");
            }
        }
        return jobRunInfo;
    }

    private JobRunInfo build(Integer commandId, Command command) {
        JobRunInfo jobRunInfo = new JobRunInfo();
        jobRunInfo.setJobId(String.valueOf(commandId));
        jobRunInfo.setJobStatus(JobStatus.RUNNING);
        jobRunInfo.setJobCompleteRate(String.format(JobRunInfo.JOB_COMPLETE_RATE_FORMAT, 0, taskSize(command.getProcessDefinitionCode(), command.getProcessDefinitionVersion())));
        return jobRunInfo;
    }

    private JobRunInfo build(Integer commandId, ErrorCommand errorCommand, ProcessDefinition processDefinition) {
        JobRunInfo jobRunInfo = new JobRunInfo();
        jobRunInfo.setJobId(String.valueOf(commandId));
        jobRunInfo.setJobStatus(JobStatus.FAILED);
        jobRunInfo.setErrorMsg("启动失败");
        if (processDefinition != null) {
            jobRunInfo.setJobCompleteRate(String.format(JobRunInfo.JOB_COMPLETE_RATE_FORMAT, 0, taskSize(errorCommand.getProcessDefinitionCode(), processDefinition.getVersion())));
        } else {
            jobRunInfo.setJobCompleteRate(String.format(JobRunInfo.JOB_COMPLETE_RATE_FORMAT, 0, 0));
        }
        return jobRunInfo;
    }

    private JobRunInfo build(Integer commandId) {
        JobRunInfo jobRunInfo = new JobRunInfo();
        jobRunInfo.setJobId(commandId.toString());
        jobRunInfo.setJobStatus(JobStatus.FAILED);
        jobRunInfo.setErrorMsg("未找到该任务");
        jobRunInfo.setJobCompleteRate(String.format(JobRunInfo.JOB_COMPLETE_RATE_FORMAT, 0, 0));
        return jobRunInfo;
    }

    private int primaryIntGet(Supplier<Integer> supplier) {
        Integer i = supplier.get();
        if (i == null) {
            return 0;
        }
        return i;
    }

    private void uploadToHdfs(String externalCode, MultipartFile[] files) throws IOException {
        if (files != null) {
            for (MultipartFile file : files) {
                if (file == null) {
                    continue;
                }
                if (StringUtils.isEmpty(file.getOriginalFilename())) {
                    continue;
                }
                try (InputStream inputStream = file.getInputStream()) {
                    HadoopUtils.getInstance().create("/tmp/material_light_handle/file/" + externalCode + "/" + file.getOriginalFilename(), inputStream);
                }
            }
        }
    }
}