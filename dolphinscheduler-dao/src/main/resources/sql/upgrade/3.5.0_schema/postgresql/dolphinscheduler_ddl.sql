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

CREATE INDEX idx_project_submit_time ON t_ds_task_instance (project_code ASC, submit_time DESC);
CREATE INDEX idx_project_start_time ON t_ds_workflow_instance (project_code ASC, start_time DESC);
ALTER TABLE t_ds_schedules
    ADD COLUMN missed_fire_policy smallint NOT NULL DEFAULT 2;

-- DSIP-110: the datasource type is now a string name declared by the datasource plugin.
-- The legacy numeric code is converted to the type name in one statement; codes without a mapping
-- (corrupted data) are converted to an UNKNOWN_<code> sentinel so they stay listable and are marked
-- unavailable, instead of being silently dropped.
ALTER TABLE t_ds_datasource
    ALTER COLUMN type TYPE varchar(64) USING (CASE type
        WHEN 0 THEN 'MYSQL'
        WHEN 1 THEN 'POSTGRESQL'
        WHEN 2 THEN 'HIVE'
        WHEN 3 THEN 'SPARK'
        WHEN 4 THEN 'CLICKHOUSE'
        WHEN 5 THEN 'ORACLE'
        WHEN 6 THEN 'SQLSERVER'
        WHEN 7 THEN 'DB2'
        WHEN 8 THEN 'PRESTO'
        WHEN 9 THEN 'H2'
        WHEN 10 THEN 'REDSHIFT'
        WHEN 11 THEN 'ATHENA'
        WHEN 12 THEN 'TRINO'
        WHEN 13 THEN 'STARROCKS'
        WHEN 14 THEN 'AZURESQL'
        WHEN 15 THEN 'DAMENG'
        WHEN 16 THEN 'OCEANBASE'
        WHEN 17 THEN 'SSH'
        WHEN 18 THEN 'KYUUBI'
        WHEN 19 THEN 'DATABEND'
        WHEN 20 THEN 'SNOWFLAKE'
        WHEN 21 THEN 'VERTICA'
        WHEN 22 THEN 'HANA'
        WHEN 23 THEN 'DORIS'
        WHEN 24 THEN 'ZEPPELIN'
        WHEN 25 THEN 'SAGEMAKER'
        WHEN 26 THEN 'K8S'
        WHEN 27 THEN 'ALIYUN_SERVERLESS_SPARK'
        WHEN 28 THEN 'DOLPHINDB'
        ELSE 'UNKNOWN_' || type
    END);
