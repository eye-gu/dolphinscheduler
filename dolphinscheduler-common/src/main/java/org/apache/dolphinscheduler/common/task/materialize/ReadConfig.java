/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import java.util.List;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 */
@Data
public class ReadConfig {
    /**
     * mysql/gaussdb/hive
     * 内部hive不需要配置readconfig对象, 所以hive代表是kylin
     */
    private String type;
    /**
     * ip
     */
    private String ip;
    /**
     * 端口
     */
    private Integer port;
    /**
     * 库
     */
    private String schema;
    /**
     * 用户名
     */
    private String userName;
    /**
     * 密码
     */
    private String password;
    /**
     * 拉取的表列表
     */
    private List<TableNode> tableList;
    /**
     * hdfs路径
     */
    private String path;
    /**
     * 元数据
     */
    private MetaData metaData;
}