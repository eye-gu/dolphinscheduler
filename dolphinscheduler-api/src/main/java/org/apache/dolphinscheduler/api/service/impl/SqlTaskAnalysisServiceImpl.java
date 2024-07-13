package org.apache.dolphinscheduler.api.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.service.SqlTaskAnalysisService;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPluginLoader;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SqlTaskAnalysisServiceImpl implements SqlTaskAnalysisService {

    private Map<String, SqlTaskAnalysisPlugin> plugins;

    @PostConstruct
    public void init() {
        SqlTaskAnalysisPluginLoader loader = new SqlTaskAnalysisPluginLoader();
        List<SqlTaskAnalysisPlugin> loadPlugins = loader.load();
        plugins = loadPlugins.stream().collect(Collectors.toMap(SqlTaskAnalysisPlugin::name, Function.identity()));
    }

    @Override
    public List<SqlTaskAnalysisPlugin> allSqlTaskAnalysisPlugins() {
        return new ArrayList<>(plugins.values());
    }

    @Override
    public SqlTaskAnalysisPlugin getSqlTaskAnalysisPlugin(String pluginName) {
        return plugins.get(pluginName);
    }
}
