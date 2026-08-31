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

/**
 * The datasource type name declared by the datasource plugin, e.g. MYSQL or the name declared by a
 * custom datasource plugin. The closed string-literal union was replaced by a plain string since
 * DSIP-110, the valid values come from GET /datasources/types at runtime.
 */
type IDataBase = string

type IDataBaseLabel = string

/**
 * The metadata of a datasource type registered by an installed datasource plugin.
 */
interface IDataSourceType {
  type: string
  label: string
  defaultPort?: number | null
  jdbcCompatible: boolean
}

interface IDataSource {
  id?: number
  type?: IDataBase
  label?: IDataBaseLabel
  name?: string
  note?: string
  host?: string
  port?: number
  principal?: string
  javaSecurityKrb5Conf?: string
  loginUserKeytabUsername?: string
  loginUserKeytabPath?: string
  mode?: string
  userName?: string
  password?: string
  awsRegion?: string
  database?: string
  connectType?: string
  other?: object
  restEndpoint?: string
  kubeConfig?: string
  namespace?: string
  MSIClientId?: string
  dbUser?: string
  compatibleMode?: string
  privateKey?: string
  datawarehouse?: string
  accessKeyId?: string
  accessKeySecret?: string
  regionId?: string
  endpoint?: string
}

interface ListReq {
  pageNo: number
  pageSize: number
  searchVal?: string
}

interface UserIdReq {
  userId: number
}

interface TypeReq {
  type: IDataBase
}

interface NameReq {
  name: string
}

type IdReq = number

export {
  ListReq,
  IDataBase,
  IDataSourceType,
  IDataSource,
  UserIdReq,
  TypeReq,
  NameReq,
  IdReq
}
