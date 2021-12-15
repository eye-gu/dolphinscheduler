/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.dto.materialize;

import org.apache.dolphinscheduler.common.task.self.Param;

import java.util.List;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 11:02
 */
@Data
public class MaterializeLightHandleProcessDefinition {

    /**
     * 工作流code, 可以传外部id
     */
    @ApiModelProperty(value = "外部id", required = true)
    private String externalCode;
    /**
     * 名字
     */
    @ApiModelProperty(value = "工作流名称", required = true)
    private String name;
    /**
     * 描述, 非必传
     */
    private String description;
    /**
     * 工作流超时时间, 单位分钟, 0不告警
     */
    private Integer timeout;

    /**
     * 所有任务执行前获取参数
     */
    private List<Param> globalParams;

    /**
     * 任务列表
     */
    private List<MaterializeLightHandleTaskDefinition> tasks;
}