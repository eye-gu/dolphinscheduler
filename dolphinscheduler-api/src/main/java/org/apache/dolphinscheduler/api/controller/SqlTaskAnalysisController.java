package org.apache.dolphinscheduler.api.controller;


import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.SqlTaskAnalysisService;
import org.apache.dolphinscheduler.api.task.SqlTaskAnalysisPlugin;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("sql-task-analysis")
@Slf4j
public class SqlTaskAnalysisController extends BaseController {

    @Autowired
    private SqlTaskAnalysisService sqlTaskAnalysisService;

    @GetMapping(value = "/plugins")
    public Result sqlParsePluginList() {
        ArrayNode arrayNode = JSONUtils.createArrayNode();
        List<SqlTaskAnalysisPlugin> plugins = sqlTaskAnalysisService.allSqlTaskAnalysisPlugins();
        plugins.forEach(plugin -> {
            ObjectNode processDefinitionNode = JSONUtils.createObjectNode();
            processDefinitionNode.put("name", plugin.name());
            arrayNode.add(processDefinitionNode);
        });
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.DATA_LIST, arrayNode);
        result.put(Constants.STATUS, Status.SUCCESS);
        return returnDataList(result);
    }
}
