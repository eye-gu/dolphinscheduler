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

package org.apache.dolphinscheduler.plugin.datasource.api.plugin;

import static java.lang.String.format;

import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.spi.datasource.DataSourceChannel;
import org.apache.dolphinscheduler.spi.datasource.DataSourceChannelFactory;
import org.apache.dolphinscheduler.spi.plugin.PrioritySPIFactory;

import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSourcePluginManager {

    private static final Map<String, DataSourceChannel> datasourceChannelMap = new ConcurrentHashMap<>();

    private static final Map<String, DataSourceProcessor> dataSourceProcessorMap = new ConcurrentHashMap<>();

    static {
        loadDataSourcePlugin();
    }

    public static DataSourceChannel getDataSourceChannel(@NonNull String type) {
        return datasourceChannelMap.get(normalizeType(type));
    }

    public static DataSourceProcessor getDataSourceProcessor(@NonNull String type) {
        return dataSourceProcessorMap.get(normalizeType(type));
    }

    /**
     * Get the datasource processor of the given type, fail with an explicit message when the datasource plugin of
     * this type is not installed. This is the uniform error path for datasources whose type plugin is missing
     * (e.g. the datasource was created by another deployment with more plugins installed).
     */
    public static DataSourceProcessor getDataSourceProcessorChecked(@NonNull String type) {
        DataSourceProcessor dataSourceProcessor = getDataSourceProcessor(type);
        if (dataSourceProcessor == null) {
            throw new IllegalArgumentException(
                    format("datasource type '%s' is not installed, please install the datasource plugin first",
                            type));
        }
        return dataSourceProcessor;
    }

    /**
     * Whether a datasource plugin declaring the given type name is installed.
     */
    public static boolean existDataSourceProcessor(@NonNull String type) {
        return getDataSourceProcessor(type) != null;
    }

    /**
     * List the metadata of all registered datasource types, sorted by type name. This is the single source of the
     * datasource type list exposed to the frontend (GET /datasources/types).
     */
    public static List<DataSourceTypeInfo> getDataSourceTypeInfoList() {
        List<DataSourceTypeInfo> typeInfos = new ArrayList<>();
        dataSourceProcessorMap.forEach((type, processor) -> typeInfos.add(new DataSourceTypeInfo(
                type,
                processor.getLabel(),
                processor.getDefaultPort(),
                processor.isJdbcCompatible())));
        typeInfos.sort(Comparator.comparing(DataSourceTypeInfo::getType));
        return typeInfos;
    }

    public static void loadDataSourcePlugin() {
        initializeDataSourceChannel();
        initializeDataSourceProcessor();
    }

    private static synchronized void initializeDataSourceChannel() {
        if (MapUtils.isNotEmpty(datasourceChannelMap)) {
            return;
        }
        new PrioritySPIFactory<>(DataSourceChannelFactory.class).getSPIMap().forEach(
                (dataSourceChannelName, dataSourceChannelFactory) -> {
                    String registerName = normalizeType(dataSourceChannelName);
                    if (datasourceChannelMap.containsKey(registerName)) {
                        throw new IllegalStateException(
                                format("Duplicate datasource channel named '%s'", registerName));
                    }
                    datasourceChannelMap.put(registerName, dataSourceChannelFactory.create());
                    log.info("Registered datasource channel: {}", registerName);
                });
    }

    private static synchronized void initializeDataSourceProcessor() {
        if (MapUtils.isNotEmpty(dataSourceProcessorMap)) {
            return;
        }

        ServiceLoader.load(DataSourceProcessor.class).forEach(factory -> {
            final String name = normalizeType(factory.getType());
            if (dataSourceProcessorMap.containsKey(name)) {
                throw new IllegalStateException(format("Duplicate datasource processor named '%s'", name));
            }
            DataSourceProcessor dataSourceProcessor = factory.create();
            dataSourceProcessorMap.put(name, dataSourceProcessor);
            log.info("Success register datasource processor -> {}", name);
        });
    }

    /**
     * Datasource type names are case-normalized to upper case, so that a type declared as "mysql" by a legacy
     * plugin and "MYSQL" submitted by the frontend resolve to the same registry entry.
     */
    private static String normalizeType(String type) {
        return type.toUpperCase(Locale.ROOT);
    }

}
