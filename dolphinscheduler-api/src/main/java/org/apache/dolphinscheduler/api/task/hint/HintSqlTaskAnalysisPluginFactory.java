package org.apache.dolphinscheduler.api.task.hint;


import com.google.auto.service.AutoService;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPluginFactory;

@AutoService(SqlTaskAnalysisPluginFactory.class)
public class HintSqlTaskAnalysisPluginFactory implements SqlTaskAnalysisPluginFactory {
    @Override
    public SqlTaskAnalysisPlugin build() {
        return new HintSqlTaskAnalysisPlugin();
    }
}
