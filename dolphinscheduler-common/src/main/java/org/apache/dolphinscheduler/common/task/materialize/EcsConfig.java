package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

@Data
public class EcsConfig {

    private String restEndPoint;

    private String accessKey;

    private String secretAccessKey;

    private String bucketName;
}
