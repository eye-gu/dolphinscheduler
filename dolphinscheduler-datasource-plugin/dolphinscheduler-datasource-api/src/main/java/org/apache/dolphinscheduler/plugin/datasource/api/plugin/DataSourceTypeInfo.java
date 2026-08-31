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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The metadata of a datasource type registered by an installed datasource plugin. This is what the
 * {@code GET /datasources/types} API returns so the frontend can render the type list dynamically instead of
 * hardcoding it.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataSourceTypeInfo {

    /**
     * The unique, case-normalized (upper case) type name, e.g. MYSQL.
     */
    private String type;

    /**
     * The human readable label shown in the UI.
     */
    private String label;

    /**
     * The suggested port used as the default value of the port field, null when the type has no port suggestion.
     */
    private Integer defaultPort;

    /**
     * Whether the datasource is accessible through a standard JDBC URL, used by the UI to decide whether the
     * generic JDBC form applies to this type.
     */
    private boolean jdbcCompatible;

}
