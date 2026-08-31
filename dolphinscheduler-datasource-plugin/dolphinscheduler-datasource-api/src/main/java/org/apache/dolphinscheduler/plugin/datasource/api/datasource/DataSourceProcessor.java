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

package org.apache.dolphinscheduler.plugin.datasource.api.datasource;

import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface DataSourceProcessor {

    /**
     * cast JSON to relate DTO
     *
     * @param paramJson
     * @return {@link BaseDataSourceParamDTO}
     */
    BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson);

    /**
     * check datasource param is valid.
     * @throws IllegalArgumentException if invalid
     */
    void checkDatasourceParam(BaseDataSourceParamDTO datasourceParam);

    /**
     * get Datasource Client UniqueId
     *
     * @return UniqueId
     */
    String getDatasourceUniqueId(ConnectionParam connectionParam);

    /**
     * create BaseDataSourceParamDTO by connectionJson
     *
     * @param connectionJson see{@link org.apache.dolphinscheduler.dao.entity.DataSource}
     * @return {@link BaseDataSourceParamDTO}
     */
    BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson);

    /**
     * create datasource connection parameter which will be stored at DataSource
     * <p>
     * see {@code org.apache.dolphinscheduler.dao.entity.DataSource.connectionParams}
     */
    ConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam);

    /**
     * deserialize json to datasource connection param
     *
     * @param connectionJson {@code org.apache.dolphinscheduler.dao.entity.DataSource.connectionParams}
     * @return {@link ConnectionParam}
     */
    ConnectionParam createConnectionParams(String connectionJson);

    /**
     * get datasource Driver
     */
    String getDatasourceDriver();

    /**
     * get validation Query
     */
    String getValidationQuery();

    /**
     * get jdbcUrl by connection param, the jdbcUrl is different with ConnectionParam.jdbcUrl, this method will inject
     * other to jdbcUrl
     *
     * @param connectionParam connection param
     */
    String getJdbcUrl(ConnectionParam connectionParam);

    /**
     * get connection by connectionParam
     *
     * @param connectionParam connectionParam
     * @return {@link Connection}
     */
    // todo: Change to return a ConnectionWrapper
    Connection getConnection(ConnectionParam connectionParam) throws SQLException, IOException;

    /**
     * test connection
     *
     * @param connectionParam connectionParam
     * @return true if connection is valid
     */
    boolean checkDataSourceConnectivity(ConnectionParam connectionParam);

    /**
     * The identity of the datasource type provided by this processor.
     * <p>
     * The name must be unique among all installed datasource plugins, case-normalized to upper case (e.g.
     * "MYSQL", "DORIS", "MY_INTERNAL_DB"). It is used to route parameters/connections to this processor, and is
     * persisted in {@code t_ds_datasource.type}.
     */
    String getType();

    /**
     * The human readable label of the datasource type, shown in the UI. Defaults to the type name.
     */
    default String getLabel() {
        return getType();
    }

    /**
     * The suggested port of the datasource type, used by the UI as the default value of the port field.
     * {@code null} means no suggestion (the port field is still rendered unless the type is not JDBC compatible).
     */
    default Integer getDefaultPort() {
        return null;
    }

    /**
     * Whether the datasource is accessible through a standard JDBC URL (host/port/database). Used by the UI to
     * decide if the generic JDBC form can be rendered for this type.
     */
    default boolean isJdbcCompatible() {
        return true;
    }

    /**
     * get datasource processor
     */
    DataSourceProcessor create();

    List<String> splitAndRemoveComment(String sql);
}
