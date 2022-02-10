package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

@Data
public class EcsConfig {

    private String endPoint;

    private String identity;

    private String secretKey;

    private String bucket;

    private String key;
}
