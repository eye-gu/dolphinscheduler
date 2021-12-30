/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.common.task.materialize.Param;
import org.apache.dolphinscheduler.common.task.materialize.ParamUtils;
import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.Sql;
import org.apache.dolphinscheduler.common.task.materialize.StoreConfig;
import org.apache.dolphinscheduler.common.utils.HadoopUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class MaterializeTask extends AbstractTaskExecutor {

    private final MaterializeParameters materializeParameters;

    private final int taskInstanceId;

    private long start;

    private final List<Property> globalParams;

    /**
     * constructor
     *
     * @param taskRequest taskRequest
     */
    protected MaterializeTask(TaskRequest taskRequest) {
        super(taskRequest);
        materializeParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), MaterializeParameters.class);
        taskInstanceId = taskRequest.getTaskInstanceId();
        globalParams = JSONUtils.toList(taskRequest.getGlobalParams(), Property.class);
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
        TaskEntity task = new TaskEntity();
        task.setReadConfig(materializeParameters.getReadConfig());
        task.setStoreConfig(materializeParameters.getStoreConfig());
        List<SqlEntity> sqlEntities = new ArrayList<>();
        for (Sql sql : materializeParameters.getSqlList()) {
            SqlEntity sqlEntity = new SqlEntity();
            sqlEntity.setSqlTemplate(sql.getSqlTemplate());
            List<ParamEntity> paramEntities = new ArrayList<>();
            sqlEntity.setParams(paramEntities);
            if (CollectionUtils.isNotEmpty(globalParams)) {
                for (Property property : globalParams) {
                    ParamEntity paramEntity = new ParamEntity();
                    paramEntity.setName(property.getProp());
                    paramEntity.setType(property.getType().name());
                    paramEntity.setValue(property.getValue());
                    paramEntities.add(paramEntity);
                }
            }
            if (CollectionUtils.isNotEmpty(sql.getParams())) {
                for (Param param : sql.getParams()) {
                    ParamEntity paramEntity = new ParamEntity();
                    paramEntity.setName(param.getName());
                    paramEntity.setType(ParamUtils.convertToDataType(param).name());
                    paramEntity.setValue(JSONUtils.toJsonString(ParamUtils.paramValue(param)));
                    paramEntities.add(paramEntity);
                }
            }
            sqlEntities.add(sqlEntity);
        }
        task.setSqlList(sqlEntities);

        String file = "/material_light_handle/" + taskInstanceId;
        HadoopUtils.getInstance().create(file, JSONUtils.toJsonString(task).getBytes(StandardCharsets.UTF_8));

        MaterializeParameters.SparkParameters sparkParameters = materializeParameters.getSparkParameters();
        SparkLauncher launcher = new SparkLauncher()
            .setSparkHome(sparkParameters.getHome())
            .setMaster(sparkParameters.getMaster())
            .setAppResource(sparkParameters.getMainJar())
            .setMainClass(sparkParameters.getMainClass())
            .addAppArgs(String.valueOf(taskInstanceId))
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
        SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
                logger.info("state change:{}", handle.getState());
            }

            @Override
            public void infoChanged(SparkAppHandle handle) {
                logger.info("info change:{}", handle.getAppId());
            }
        });
        while (!handle.getState().isFinal()) {
            if (Thread.currentThread().isInterrupted()) {
                logger.error("thread interrupt");
                handle.stop();
                HadoopUtils.getInstance().delete(file, true);
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
                    logger.error("maximum limit exceeded for 8 hours");
                    handle.stop();
                    HadoopUtils.getInstance().delete(file, true);
                    return false;
                }
            } else {
                if (System.currentTimeMillis() - start > materializeParameters.getTimeout() * 60 * 1000) {
                    logger.error("timeout {} minutes", materializeParameters.getTimeout());
                    handle.stop();
                    HadoopUtils.getInstance().delete(file, true);
                    return false;
                }
            }
        }
        if (!handle.getState().equals(SparkAppHandle.State.FINISHED)) {
            logger.error("spark job failed:{}", handle.getState());
            handle.getError().ifPresent(e -> logger.error("spark job failed", e));
            HadoopUtils.getInstance().delete(file, true);
            return false;
        }
        HadoopUtils.getInstance().delete(file, true);
        return true;
    }

    private boolean asyncPlatform() {
        // todo
        return true;
    }


    @Data
    private static class TaskEntity {
        private ReadConfig readConfig;
        private StoreConfig storeConfig;
        private List<SqlEntity> sqlList;
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