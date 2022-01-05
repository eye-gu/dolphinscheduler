package org.apache.dolphinscheduler.common.task.materialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.dolphinscheduler.common.enums.DataType;

@AllArgsConstructor
@Getter
public enum ParamTypeEnum {

    INTEGER(DataType.INTEGER),

    REAL(DataType.DOUBLE),

    STRING(DataType.VARCHAR),

    DATE(DataType.DATE),

    DATETIME(DataType.TIMESTAMP),

    ;

    private DataType dataType;
}
