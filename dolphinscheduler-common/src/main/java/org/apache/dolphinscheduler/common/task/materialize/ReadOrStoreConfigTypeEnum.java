package org.apache.dolphinscheduler.common.task.materialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ReadOrStoreConfigTypeEnum {

    MYSQL,

    GAUSSDB,

    HIVE,

    FILE,

    ECS,
    ;
}
