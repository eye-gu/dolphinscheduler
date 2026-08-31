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

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.google.auto.service.AutoService;

/**
 * The H2 datasource plugin is the reference implementation of an "externally developed" datasource plugin
 * (DSIP-110): it is a plain JDBC plugin that declares its own datasource type identity via {@link #getType()},
 * bundles its driver, and is registered through META-INF/services.
 */
@AutoService(DataSourceProcessor.class)
public class H2DataSourceProcessor extends AbstractDataSourceProcessor {

    private static final String JDBC_URL_HEADER = "jdbc:h2:tcp://";

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, H2DataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        H2ConnectionParam connectionParams = (H2ConnectionParam) createConnectionParams(connectionJson);
        H2DataSourceParamDTO h2DatasourceParamDTO = new H2DataSourceParamDTO();

        h2DatasourceParamDTO.setUserName(connectionParams.getUser());
        h2DatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        h2DatasourceParamDTO.setOther(connectionParams.getOther());

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        h2DatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        h2DatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);

        return h2DatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        H2DataSourceParamDTO h2DatasourceParam = (H2DataSourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", JDBC_URL_HEADER, h2DatasourceParam.getHost(),
                h2DatasourceParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, h2DatasourceParam.getDatabase());

        H2ConnectionParam h2ConnectionParam = new H2ConnectionParam();
        h2ConnectionParam.setJdbcUrl(jdbcUrl);
        h2ConnectionParam.setDatabase(h2DatasourceParam.getDatabase());
        h2ConnectionParam.setAddress(address);
        h2ConnectionParam.setUser(h2DatasourceParam.getUserName());
        h2ConnectionParam.setPassword(PasswordUtils.encodePassword(h2DatasourceParam.getPassword()));
        h2ConnectionParam.setDriverClassName(getDatasourceDriver());
        h2ConnectionParam.setValidationQuery(getValidationQuery());
        h2ConnectionParam.setOther(h2DatasourceParam.getOther());

        return h2ConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, H2ConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return "org.h2.Driver";
    }

    @Override
    public String getValidationQuery() {
        return "select 1";
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        H2ConnectionParam h2ConnectionParam = (H2ConnectionParam) connectionParam;
        if (MapUtils.isNotEmpty(h2ConnectionParam.getOther())) {
            String otherParams = transformOther(h2ConnectionParam.getOther());
            return String.format("%s?%s", h2ConnectionParam.getJdbcUrl(), otherParams);
        }
        return h2ConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws SQLException {
        H2ConnectionParam h2ConnectionParam = (H2ConnectionParam) connectionParam;
        try {
            return java.sql.DriverManager.getConnection(getJdbcUrl(h2ConnectionParam),
                    h2ConnectionParam.getUser(),
                    PasswordUtils.decodePassword(h2ConnectionParam.getPassword()));
        } catch (Exception e) {
            throw new SQLException("get H2 connection error", e);
        }
    }

    @Override
    public String getType() {
        return DbType.H2.name();
    }

    @Override
    public Integer getDefaultPort() {
        return 9092;
    }

    @Override
    public DataSourceProcessor create() {
        return new H2DataSourceProcessor();
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isNotEmpty(otherMap)) {
            StringBuilder stringBuilder = new StringBuilder();
            otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s&", key, value)));
            return stringBuilder.substring(0, stringBuilder.length() - 1);
        }
        return null;
    }
}
