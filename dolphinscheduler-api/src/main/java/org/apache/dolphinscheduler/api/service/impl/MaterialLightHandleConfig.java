/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.service.impl;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
@ConfigurationProperties(
    prefix = "material.light.handle"
)
@Component
public class MaterialLightHandleConfig {

    private Integer userId;
    private Long projectCode;
    private Integer tenantId;
    private Integer warningGroupId;

    private SparkConfig spark;

    private AsyncPlatformConfig asyncPlatform;


    @Data
    public static class AsyncPlatformConfig {

    }


    @Data
    public static class SparkConfig {
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
}