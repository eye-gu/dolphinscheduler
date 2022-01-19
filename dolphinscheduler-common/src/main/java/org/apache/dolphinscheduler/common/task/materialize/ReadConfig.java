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
     * mysql/gaussdb/hive/file
     * 内部hive不需要配置readconfig对象, 所以hive代表是kylin
     * @see ReadOrStoreConfigTypeEnum
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
     * 数据库
     */
    private String database;
    /**
     * 用户名
     */
    private String userName;
    /**
     * 密码
     */
    private String password;
    /**
     * 数据源id / 文件名
     * 确定一个唯一的数据源信息, 和传入是的信息映射
     */
    private String datasourceId;
    /**
     * 拉取的表列表
     */
    private List<TableNode> tableList;
    /**
     * hdfs路径
     */
    private String path;

    private String fileType;
    /**
     * 元数据
     */
    private MetaData metaData;
}