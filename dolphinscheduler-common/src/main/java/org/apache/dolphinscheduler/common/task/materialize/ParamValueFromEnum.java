package org.apache.dolphinscheduler.common.task.materialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ParamValueFromEnum {

    SQL_QUERY,

    CONSTANT,

    FUNCTION,

    EXEC_PARAM,
    ;
}
