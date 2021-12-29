/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.dto.materialize;

import org.apache.dolphinscheduler.common.task.materialize.ReadConfig;
import org.apache.dolphinscheduler.common.task.materialize.Sql;
import org.apache.dolphinscheduler.common.task.materialize.StoreConfig;

import java.util.List;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class MaterializeLightHandleTaskDefinition {

    /**
     * 外部id
     */
    private String externalCode;
    /**
     * 名字
     */
    private String name;
    /**
     * 描述, 非必传
     */
    private String description;
    /**
     * 延时运行时间, 立即运行传0
     */
    private Integer delayTime;
    /**
     * 失败重试次数, 0不重试
     */
    private Integer failRetryTimes;
    /**
     * 失败重试间隔, 单位分钟
     */
    private Integer failRetryInterval;
    /**
     * 任务超时时间, 单位分钟, 0不告警
     */
    private Integer timeout;

    /**
     * 父任务code列表
     */
    private List<String> preExternalCodes;

    /**
     * 外部数据源, HIVE代表是kylin数据源
     */
    private ReadConfig readConfig;
    private StoreConfig storeConfig;
    private List<Sql> sqlLists;
}