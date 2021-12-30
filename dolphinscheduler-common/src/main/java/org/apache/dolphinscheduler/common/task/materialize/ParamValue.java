package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import java.util.List;

@Data
public class ParamValue {

    /**
     * 来源
     * SQL_QUERY CONSTANT FUNCTION
     */
    private String from;

    /**
     * 相关配置
     */
    private String config;

    /**
     * 默认值
     */
    private String defaultValue;
    /**
     * 读取配置
     */
    private ReadConfig readConfig;
    /**
     * 子参数, 可以配置替换config中的占位符
     */
    private List<Param> childParams;
}
