package org.apache.dolphinscheduler.common.task.materialize;


public enum JobStatus {
    RUNNING("R"),
    FAILED("F"),
    STOP("stop"),
    SUCCESS("S"),
    ;

    private String param;

    JobStatus(String param) {
        this.param = param;
    }
    public String getVal() {
        return this.param;
    }
}
