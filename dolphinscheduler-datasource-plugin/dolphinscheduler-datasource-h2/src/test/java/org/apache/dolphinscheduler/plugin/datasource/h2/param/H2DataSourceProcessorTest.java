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

package org.apache.dolphinscheduler.plugin.datasource.h2.param;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourcePluginManager;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class H2DataSourceProcessorTest {

    private final H2DataSourceProcessor processor = new H2DataSourceProcessor();

    @Test
    public void testGetType() {
        Assertions.assertEquals(DbType.H2.name(), processor.getType());
        Assertions.assertEquals(9092, processor.getDefaultPort());
        Assertions.assertTrue(processor.isJdbcCompatible());
    }

    @Test
    public void testCreateConnectionParams() {
        Map<String, String> other = new HashMap<>();
        other.put("SERVER_TIMEZONE", "Asia/Shanghai");
        H2DataSourceParamDTO h2DataSourceParamDTO = new H2DataSourceParamDTO();
        h2DataSourceParamDTO.setHost("localhost");
        h2DataSourceParamDTO.setPort(9092);
        h2DataSourceParamDTO.setUserName("root");
        h2DataSourceParamDTO.setPassword("123456");
        h2DataSourceParamDTO.setDatabase("test");
        h2DataSourceParamDTO.setOther(other);

        H2ConnectionParam connectionParams = (H2ConnectionParam) processor.createConnectionParams(h2DataSourceParamDTO);
        Assertions.assertEquals("jdbc:h2:tcp://localhost:9092/test", connectionParams.getJdbcUrl());
        Assertions.assertEquals("org.h2.Driver", connectionParams.getDriverClassName());
    }

    @Test
    public void testCreateDatasourceParamDTO() {
        H2ConnectionParam connectionParam = new H2ConnectionParam();
        connectionParam.setJdbcUrl("jdbc:h2:tcp://localhost:9092/test");
        connectionParam.setAddress("jdbc:h2:tcp://localhost:9092");
        connectionParam.setUser("root");
        connectionParam.setPassword("123456");
        connectionParam.setDatabase("test");

        H2DataSourceParamDTO dataSourceParamDTO =
                (H2DataSourceParamDTO) processor.createDatasourceParamDTO(JSONUtils.toJsonString(connectionParam));
        Assertions.assertEquals("localhost", dataSourceParamDTO.getHost());
        Assertions.assertEquals(9092, dataSourceParamDTO.getPort());
        Assertions.assertEquals("test", dataSourceParamDTO.getDatabase());
    }

    @Test
    public void testRegisterInPluginManager() {
        Assertions.assertNotNull(DataSourcePluginManager.getDataSourceProcessor("H2"));
        Assertions.assertSame(processor.getClass(), DataSourcePluginManager.getDataSourceProcessor("h2").getClass());
        Assertions.assertTrue(DataSourcePluginManager.existDataSourceProcessor("H2"));
        Assertions.assertFalse(DataSourcePluginManager.existDataSourceProcessor("NOT_INSTALLED_TYPE"));
    }

    @Test
    public void testGetDatasourceUniqueId() {
        H2ConnectionParam connectionParam = new H2ConnectionParam();
        connectionParam.setJdbcUrl("jdbc:h2:tcp://localhost:9092/test");
        connectionParam.setUser("root");
        connectionParam.setPassword("123456");

        ConnectionParam cp = processor.createConnectionParams(JSONUtils.toJsonString(connectionParam));
        String uniqueId = processor.getDatasourceUniqueId(cp);
        Assertions.assertNotNull(uniqueId);
        Assertions.assertTrue(uniqueId.startsWith("H2@"));
    }

    @Test
    public void testGetDatasourceProcessorCheckedThrowsForUnknownType() {
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DataSourcePluginManager.getDataSourceProcessorChecked("NOT_INSTALLED_TYPE"));
        Assertions.assertTrue(exception.getMessage().contains("NOT_INSTALLED_TYPE"));
    }
}
