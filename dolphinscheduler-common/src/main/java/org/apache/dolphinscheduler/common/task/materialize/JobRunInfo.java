package org.apache.dolphinscheduler.common.task.materialize;


import lombok.Data;

@Data
public class JobRunInfo {

    public static final String JOB_COMPLETE_RATE_FORMAT = "成功节点/总节点:%s/%s";

    private String jobId;

    private JobStatus jobStatus;

    /**
     * 成功节点/总节点:%s/%s
     */
    private String jobCompleteRate;

    private String errorMsg;
}
