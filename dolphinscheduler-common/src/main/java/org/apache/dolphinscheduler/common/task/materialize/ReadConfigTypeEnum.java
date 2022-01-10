package org.apache.dolphinscheduler.common.task.materialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ReadConfigTypeEnum {

    MYSQL,

    GAUSSDB,

    HIVE,

    ECS,
    ;
}
