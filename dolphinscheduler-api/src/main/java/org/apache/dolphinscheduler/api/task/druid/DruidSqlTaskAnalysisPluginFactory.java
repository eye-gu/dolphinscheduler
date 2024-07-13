package org.apache.dolphinscheduler.api.task.druid;


import com.google.auto.service.AutoService;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPluginFactory;

@AutoService(SqlTaskAnalysisPluginFactory.class)
public class DruidSqlTaskAnalysisPluginFactory implements SqlTaskAnalysisPluginFactory {
    @Override
    public SqlTaskAnalysisPlugin build() {
        return new DruidSqlTaskAnalysisPlugin();
    }
}
