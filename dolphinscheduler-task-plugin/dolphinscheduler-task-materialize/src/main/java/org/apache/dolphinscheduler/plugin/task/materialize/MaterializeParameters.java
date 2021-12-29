/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.task.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.Sql;
import org.apache.dolphinscheduler.common.task.materialize.StoreConfig;
import org.apache.dolphinscheduler.common.task.spark.SparkParameters;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.ResourceInfo;

import java.util.Collections;
import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class MaterializeParameters extends AbstractParameters {
    private ReadConfig readConfig;
    private StoreConfig storeConfig;
    private List<Sql> sqlList;

    private SparkParameters sparkParameters;

    private Integer timeout;



    @Data
    public static class SparkParameters {
        /**
         * jar file
         */
        private String mainJar;

        /**
         * main class
         */
        private String mainClass;

        /**
         * deploy mode
         */
        private String deployMode;

        private String master;

        private String home;

        /**
         * driver-cores Number of cores used by the driver, only in cluster mode
         */
        private Integer driverCores;

        /**
         * driver-memory Memory for driver
         */

        private String driverMemory;

        /**
         * num-executors Number of executors to launch
         */
        private Integer numExecutors;

        /**
         * executor-cores Number of cores per executor
         */
        private Integer executorCores;

        /**
         * Memory per executor
         */
        private String executorMemory;
    }

    @Override
    public boolean checkParameters() {
        return true;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return Collections.emptyList();
    }
}