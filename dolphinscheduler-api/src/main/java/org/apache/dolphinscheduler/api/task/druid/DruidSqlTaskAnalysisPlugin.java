package org.apache.dolphinscheduler.api.task.druid;

import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;

public class DruidSqlTaskAnalysisPlugin implements SqlTaskAnalysisPlugin {
    @Override
    public String name() {
        return "druid parser";
    }
}
