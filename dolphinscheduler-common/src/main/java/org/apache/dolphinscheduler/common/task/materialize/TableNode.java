package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

@Data
public class TableNode {
    /**
     * 表名
     */
    private String tableName;
    /**
     * 查询sql
     */
    private String selectSql;
}
