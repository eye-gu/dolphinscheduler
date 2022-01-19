package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import java.util.List;

@Data
public class ParamValue {

    /**
     * 来源
     * SQL_QUERY : sql查询
     * CONSTANT : 常量
     * FUNCTION : 函数
     * EXEC_PARAM : 执行入参
     * @see ParamValueFromEnum
     */
    private String from;

    /**
     * 相关配置
     * SQL_QUERY: sql语句
     * CONSTANT : 常量字符串: 1 / 2021-12-01
     * FUNCTION : FUNCTION_CONFIG对象的json
     * EXEC_PARAM : 应用传入参数的key
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
