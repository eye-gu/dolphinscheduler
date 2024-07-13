package org.apache.dolphinscheduler.api.task.hint;

import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;

public class HintSqlTaskAnalysisPlugin implements SqlTaskAnalysisPlugin {
    @Override
    public String name() {
        return "hint";
    }
}
