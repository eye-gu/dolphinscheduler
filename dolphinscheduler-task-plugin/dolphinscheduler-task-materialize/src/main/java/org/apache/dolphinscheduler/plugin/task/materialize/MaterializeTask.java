/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
public class MaterializeTask extends AbstractTaskExecutor {

    private MaterializeParameters materializeParameters;

    private SparkAppHandle handle;

    private int taskInstanceId;

    private long start;

    private List<Property> globalParams;

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
//        HadoopUtils.getInstance().create("/a/" + taskInstanceId, "a".getBytes(StandardCharsets.UTF_8));

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
        handle = launcher.startApplication(new SparkAppHandle.Listener() {
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

    private boolean asyncPlatform() {
        // todo
        return true;
    }
}