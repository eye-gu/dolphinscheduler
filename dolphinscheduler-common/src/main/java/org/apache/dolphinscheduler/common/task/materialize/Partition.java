package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

@Data
public class Partition {

    private String partitionName;
    private String condition;
    private String threshold;
}
