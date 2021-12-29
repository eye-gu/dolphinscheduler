package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import java.util.List;

@Data
public class PartiotionConfig {

    private String filedName;
    private String type;
    private List<Partition> partitions;
}
