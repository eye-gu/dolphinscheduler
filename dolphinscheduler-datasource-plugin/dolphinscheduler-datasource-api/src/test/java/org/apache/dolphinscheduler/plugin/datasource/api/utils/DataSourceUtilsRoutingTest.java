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

import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourcePluginManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the string-typed processor routing and the explicit error path for datasource types whose
 * plugin is not installed (DSIP-110).
 */
public class DataSourceUtilsRoutingTest {

    private static final String NOT_INSTALLED_TYPE = "NOT_INSTALLED_TYPE";

    @Test
    public void testGetDatasourceProcessorReturnsNullForUnknownType() {
        Assertions.assertNull(DataSourceUtils.getDatasourceProcessor(NOT_INSTALLED_TYPE));
        Assertions.assertFalse(DataSourcePluginManager.existDataSourceProcessor(NOT_INSTALLED_TYPE));
    }

    @Test
    public void testGetDatasourceProcessorCheckedFailsWithExplicitMessage() {
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DataSourceUtils.getDatasourceProcessorChecked(NOT_INSTALLED_TYPE));
        Assertions.assertTrue(exception.getMessage().contains(NOT_INSTALLED_TYPE));
        Assertions.assertTrue(exception.getMessage().contains("not installed"));
    }

    @Test
    public void testBuildDatasourceParamRejectsUnknownType() {
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DataSourceUtils.buildDatasourceParam("{\"type\":\"" + NOT_INSTALLED_TYPE + "\"}"));
        Assertions.assertTrue(exception.getMessage().contains(NOT_INSTALLED_TYPE));
    }

    @Test
    public void testBuildDatasourceParamRoutesByCaseInsensitiveType() {
        // The registry is empty in this module, a routed (installed) type cannot be resolved here;
        // the routing-by-name and normalization behavior is covered by the plugin module tests
        // (see H2DataSourceProcessorTest#testRegisterInPluginManager).
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DataSourceUtils.buildDatasourceParam("{\"type\":\"mysql\"}"));
        Assertions.assertTrue(exception.getMessage().contains("mysql"));
    }
}
