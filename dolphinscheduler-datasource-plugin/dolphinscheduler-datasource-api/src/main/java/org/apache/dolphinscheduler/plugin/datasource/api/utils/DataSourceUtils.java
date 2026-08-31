/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.api.utils;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourcePluginManager;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;

import java.sql.Connection;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;

@Slf4j
public class DataSourceUtils {

    public DataSourceUtils() {
    }

    /**
     * check datasource param
     *
     * @param baseDataSourceParamDTO datasource param
     */
    public static void checkDatasourceParam(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        getDatasourceProcessorChecked(baseDataSourceParamDTO.getType())
                .checkDatasourceParam(baseDataSourceParamDTO);
    }

    public static ConnectionParam buildConnectionParams(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        return getDatasourceProcessorChecked(baseDataSourceParamDTO.getType())
                .createConnectionParams(baseDataSourceParamDTO);
    }

    public static ConnectionParam buildConnectionParams(String type, String connectionJson) {
        return getDatasourceProcessorChecked(type).createConnectionParams(connectionJson);
    }

    public static String getJdbcUrl(String type, ConnectionParam baseConnectionParam) {
        return getDatasourceProcessorChecked(type).getJdbcUrl(baseConnectionParam);
    }

    public static Connection getConnection(String type, ConnectionParam connectionParam) {
        try {
            return getDatasourceProcessorChecked(type).getConnection(connectionParam);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDatasourceDriver(String type) {
        return getDatasourceProcessorChecked(type).getDatasourceDriver();
    }

    public static BaseDataSourceParamDTO buildDatasourceParamDTO(String type, String connectionParams) {
        return getDatasourceProcessorChecked(type).createDatasourceParamDTO(connectionParams);
    }

    /**
     * Get the datasource processor of the given type name, returns null when the datasource plugin of this type is
     * not installed.
     */
    public static DataSourceProcessor getDatasourceProcessor(String type) {
        return DataSourcePluginManager.getDataSourceProcessor(type);
    }

    /**
     * Get the datasource processor of the given type name, fail with an explicit message when the datasource
     * plugin of this type is not installed.
     */
    public static DataSourceProcessor getDatasourceProcessorChecked(String type) {
        return DataSourcePluginManager.getDataSourceProcessorChecked(type);
    }

    /**
     * get datasource UniqueId
     */
    public static String getDatasourceUniqueId(ConnectionParam connectionParam, String type) {
        return getDatasourceProcessorChecked(type).getDatasourceUniqueId(connectionParam);
    }

    /**
     * build connection url
     */
    public static BaseDataSourceParamDTO buildDatasourceParam(String param) {
        JsonNode jsonNodes = JSONUtils.parseObject(param);
        String type = jsonNodes.get("type").asText();

        return DataSourcePluginManager.getDataSourceProcessorChecked(type).castDatasourceParamDTO(param);
    }
}
