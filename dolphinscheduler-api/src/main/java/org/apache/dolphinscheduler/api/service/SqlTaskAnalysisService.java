package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;

import java.util.List;

public interface SqlTaskAnalysisService {

    List<SqlTaskAnalysisPlugin> allSqlTaskAnalysisPlugins();


    SqlTaskAnalysisPlugin getSqlTaskAnalysisPlugin(String pluginName);
}
