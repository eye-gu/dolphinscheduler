package org.apache.dolphinscheduler.api.task;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class SqlTaskAnalysisPluginLoader {

    public List<SqlTaskAnalysisPlugin> load() {

        List<SqlTaskAnalysisPlugin> plugins = new ArrayList<>();
        for (SqlTaskAnalysisPluginFactory sqlTaskAnalysisPluginFactory : ServiceLoader.load(SqlTaskAnalysisPluginFactory.class)) {
            SqlTaskAnalysisPlugin plugin = sqlTaskAnalysisPluginFactory.build();
            plugins.add(plugin);
        }
        return plugins;
    }
}
