/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.common.task.materialize.JSONUtils;
import org.apache.dolphinscheduler.common.task.materialize.Param;
import org.apache.dolphinscheduler.common.task.materialize.ParamUtils;
import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.ReadOrStoreConfigTypeEnum;
import org.apache.dolphinscheduler.common.task.materialize.Sql;
import org.apache.dolphinscheduler.common.task.materialize.StoreConfig;
import org.apache.dolphinscheduler.common.utils.HadoopUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class MaterializeTask extends AbstractTaskExecutor {

    private final MaterializeParameters materializeParameters;

    private final int processInstanceId;

    private final int taskInstanceId;

    private long start;

    private final List<Property> globalParams;

    private final String executePath;

    /**
     * constructor
     *
     * @param taskRequest taskRequest
     */
    protected MaterializeTask(TaskRequest taskRequest) {
        super(taskRequest);
        processInstanceId = taskRequest.getProcessInstanceId();
        materializeParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), MaterializeParameters.class);
        taskInstanceId = taskRequest.getTaskInstanceId();
        globalParams = JSONUtils.toList(taskRequest.getGlobalParams(), Property.class);
        executePath = taskRequest.getExecutePath();
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void handle() throws Exception {
        start = System.currentTimeMillis();
        if (spark()) {
            super.setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
        }


//        ReadConfig readConfig = materializeParameters.getReadConfig();
//        if (readConfig != null) {
//            if ("HIVE".equalsIgnoreCase(readConfig.getType())) {
//                if (asyncPlatform()) {
//                    super.setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
//                    return;
//                }
//            }
//        }
//
//        if (spark()) {
//            super.setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
//        }
    }

    @Override
    public AbstractParameters getParameters() {
        return materializeParameters;
    }


    @Override
    public void cancelApplication(boolean status) throws Exception {
        super.cancelApplication(status);
    }

    private boolean spark() throws Exception {
        TaskEntity task = buildTaskEntity();

        String str = JSONUtils.toJsonString(task);
        HadoopUtils.getInstance().create(buildHdfsValueFilePath(), str.getBytes(StandardCharsets.UTF_8));

        SparkAppHandle handle = buildAndStartSpark();
        boolean result = sparkRunComplete(handle);

        cleanHdfs();
        return result;
    }

    private TaskEntity buildTaskEntity() throws Exception {
        Map<String, String> globalParamsMap = Collections.emptyMap();
        if (CollectionUtils.isNotEmpty(globalParams)) {
            globalParamsMap = globalParams.stream().collect(Collectors.toMap(Property::getProp, Property::getValue));
        }

        TaskEntity task = new TaskEntity();
        ReadConfig readConfig = materializeParameters.getReadConfig();
        task.setReadConfig(handleReadConfig(globalParamsMap, readConfig));
        StoreConfig storeConfig = materializeParameters.getStoreConfig();
        task.setStoreConfig(handleStoreConfig(globalParamsMap, storeConfig));
        List<SqlEntity> sqlEntities = new ArrayList<>();
        for (Sql sql : materializeParameters.getSqlList()) {
            SqlEntity sqlEntity = new SqlEntity();
            sqlEntity.setSqlTemplate(sql.getSqlTemplate());
            sqlEntity.setParams(buildSqlParamEntity(globalParamsMap, sql.getParams()));
            sqlEntities.add(sqlEntity);
        }
        task.setSqlList(sqlEntities);
        String dryRun = globalParamsMap.get(ParamUtils.DRY_RUN);
        if (Boolean.TRUE.toString().equalsIgnoreCase(dryRun)) {
            task.setRunEmpty(true);
        }
        String allTableNames = globalParamsMap.get(ParamUtils.ALL_HIVE_TABLE_NAMES);
        if (StringUtils.isNotBlank(allTableNames)) {
            task.setAllHiveTableNames(JSONUtils.toList(allTableNames, String.class));
        }
        return task;
    }

    private List<ParamEntity> buildSqlParamEntity(Map<String, String> globalParamsMap, List<Param> sqlParams) throws Exception {
        List<ParamEntity> paramEntities = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(globalParams)) {
            for (Property property : globalParams) {
                if (property.getProp().startsWith(ParamUtils.SYSTEM_PARAM_PREFIX)) {
                    continue;
                }
                if (ParamUtils.INVALID_IN_BIX.equalsIgnoreCase(property.getValue())) {
                    continue;
                }
                ParamEntity paramEntity = new ParamEntity();
                paramEntity.setName(property.getProp());
                paramEntity.setType(property.getType().name());
                paramEntity.setValue(property.getValue());
                paramEntities.add(paramEntity);
            }
        }
        if (CollectionUtils.isNotEmpty(sqlParams)) {
            for (Param param : sqlParams) {
                String type = ParamUtils.convertToDataType(param).name();
                String value = ParamUtils.paramStrValue(globalParamsMap, param);
                if (value == null) {
                    logger.info("{} type {} value is null", param.getName(), type);
                    continue;
                }
                ParamEntity paramEntity = new ParamEntity();
                paramEntity.setName(param.getName());
                paramEntity.setType(type);
                paramEntity.setValue(value);
                logger.info("{} type {} value {}", param.getName(), type, value);
                paramEntities.add(paramEntity);
            }
        }
        return paramEntities;
    }

    private StoreConfig handleStoreConfig(Map<String, String> globalParamsMap, StoreConfig storeConfig) {
        if (storeConfig == null) {
            return null;
        }
        if (ReadOrStoreConfigTypeEnum.GAUSSDB.name().equalsIgnoreCase(storeConfig.getType())
                || ReadOrStoreConfigTypeEnum.ECS.name().equalsIgnoreCase(storeConfig.getType())) {
            StoreConfig execStoreConfig = JSONUtils.parseObject(globalParamsMap.get(ParamUtils.RESULT_STORE_CONFIG), StoreConfig.class);
            if (execStoreConfig == null) {
                return storeConfig;
            }
            ParamUtils.merge(storeConfig, execStoreConfig);
        }
        return storeConfig;
    }

    private ReadConfig handleReadConfig(Map<String, String> globalParamsMap, ReadConfig readConfig) {
        if (readConfig == null) {
            return null;
        }
        ReadConfig execReadConfig = JSONUtils.parseObject(globalParamsMap.get(ParamUtils.READ_CONFIG + readConfig.getDatasourceId()), ReadConfig.class);
        if (execReadConfig == null) {
            return readConfig;
        }
        ParamUtils.merge(readConfig, execReadConfig);
        return readConfig;
    }

    private SparkAppHandle buildAndStartSpark() throws IOException {
        MaterializeParameters.SparkParameters sparkParameters = materializeParameters.getSparkParameters();
        SparkLauncher launcher = new SparkLauncher()
            .setSparkHome(sparkParameters.getHome())
            .setMaster(sparkParameters.getMaster())
            .setAppResource(sparkParameters.getMainJar())
            .setMainClass(sparkParameters.getMainClass())
            .addAppArgs(String.valueOf(taskInstanceId), String.valueOf(processInstanceId))
            .setDeployMode(sparkParameters.getDeployMode());
        if (sparkParameters.getDriverMemory() != null) {
            launcher.setConf("spark.driver.memory", sparkParameters.getDriverMemory());
        }
        if (sparkParameters.getExecutorMemory() != null) {
            launcher.setConf("spark.executor.memory", sparkParameters.getExecutorMemory());
        }
        if (sparkParameters.getExecutorCores() != 0) {
            launcher.setConf("spark.executor.cores", String.valueOf(sparkParameters.getExecutorCores()));
        }
        if (sparkParameters.getDriverCores() != 0) {
            launcher.setConf("spark.driver.cores", String.valueOf(sparkParameters.getDriverCores()));
        }
        if (sparkParameters.getNumExecutors() != 0) {
            launcher.addSparkArg("--total-executor-cores", String.valueOf(sparkParameters.getNumExecutors()));
        }
        logger.info("start application");
        return launcher.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle1) {
                logger.info("state change:{}", handle1.getState());
            }

            @Override
            public void infoChanged(SparkAppHandle handle1) {
                logger.info("info change:{}", handle1.getAppId());
            }
        });
    }

    private boolean sparkRunComplete(SparkAppHandle handle) {
        while (!handle.getState().isFinal()) {
            if (Thread.currentThread().isInterrupted()) {
                logger.error("thread interrupt");
                handle.stop();
                return false;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // timeout
            if (materializeParameters.getTimeout() == null || materializeParameters.getTimeout().equals(0)) {
                if (System.currentTimeMillis() - start > 8 * 60 * 60 * 1000) {
                    logger.error("exceeded maximum limit for 8 hours");
                    handle.stop();
                    return false;
                }
            } else {
                if (System.currentTimeMillis() - start > materializeParameters.getTimeout() * 60 * 1000) {
                    logger.error("timeout {} minutes", materializeParameters.getTimeout());
                    handle.stop();
                    return false;
                }
            }
        }
        if (!handle.getState().equals(SparkAppHandle.State.FINISHED)) {
            logger.error("spark job failed:{}", handle.getState());
            handle.getError().ifPresent(e -> logger.error("spark job failed", e));
            return false;
        }
        return true;
    }

    private String buildHdfsValueFilePath() {
        return "/tmp/material_light_handle/" + taskInstanceId;
    }

    private void cleanHdfs() {
        try {
            logger.info("clean hdfs...");
            HadoopUtils.getInstance().delete(buildHdfsValueFilePath(), true);
        } catch (Exception e) {
            logger.error("clean hdfs failed", e);
        }
    }

    private boolean asyncPlatform() {
        // todo
        return true;
    }

    private void downloadToLocal(String url, String localFileName) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        try (CloseableHttpResponse httpResponse = httpClient.execute(get)) {
            HttpEntity entity = httpResponse.getEntity();
            InputStream inputStream = entity.getContent();
            IOUtils.copy(inputStream, new FileOutputStream(localFileName));
        }
    }


    @Data
    private static class TaskEntity {
        private ReadConfig readConfig;
        private StoreConfig storeConfig;
        private List<SqlEntity> sqlList;
        private Boolean runEmpty;
        private List<String> allHiveTableNames;
    }

    @Data
    private static class SqlEntity {
        private String sqlTemplate;
        private List<ParamEntity> params;
    }

    @Data
    private static class ParamEntity {
        private String name;
        // array_integer
        // integer long float boolean date time
        private String type;
        private String value;
    }
}