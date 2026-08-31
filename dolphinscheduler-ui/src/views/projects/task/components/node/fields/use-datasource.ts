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

import { ref, onMounted, nextTick, Ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { queryDataSourceList, queryDataSourceTypes } from '@/service/modules/data-source'
import { indexOf, find } from 'lodash'
import type { IJsonItem } from '../types'
import type { TypeReq } from '@/service/modules/data-source/types'

/**
 * Types that are not selectable in task nodes by default: they are not general purpose SQL engines
 * (SSH is a remote shell channel, K8S/SNOWFLAKE were never offered by the task selector before the
 * type list became dynamic). An explicit supportedDatasourceType whitelist overrides this.
 */
const defaultUnsupportedTaskTypes = ['SSH', 'K8S', 'SNOWFLAKE']

export function useDatasource(
  model: { [field: string]: any },
  params: {
    supportedDatasourceType?: string[]
    typeField?: string
    sourceField?: string
    span?: Ref | number
  } = {}
): IJsonItem[] {
  const { t } = useI18n()

  const options = ref([] as { label: string; value: string }[])
  const datasourceOptions = ref([] as { label: string; value: number }[])

  const getDatasourceTypes = async () => {
    // The type list comes from the backend so custom datasource plugins show up automatically
    let types: string[] = []
    try {
      const typeInfos = await queryDataSourceTypes()
      types = typeInfos.map((typeInfo) => typeInfo.type)
    } catch (error) {
      console.warn('Failed to load datasource types from backend', error)
      return
    }
    options.value = types
      .filter((type) => {
        if (params.supportedDatasourceType) {
          return indexOf(params.supportedDatasourceType, type) !== -1
        }
        return indexOf(defaultUnsupportedTaskTypes, type) === -1
      })
      .map((type) => ({ label: type, value: type }))
  }

  const refreshOptions = async () => {
    const parameters = {
      type: model[params.typeField || 'type']
    } as TypeReq
    const res = await queryDataSourceList(parameters)
    datasourceOptions.value = res.map((item: any) => ({
      label: item.name,
      value: item.id
    }))
    const sourceField = params.sourceField || 'datasource'
    if (!res.length && model[sourceField]) model[sourceField] = null
    if (res.length && model[sourceField]) {
      const item = find(res, { id: model[sourceField] })
      if (!item) {
        model[sourceField] = null
      }
    }
  }

  const onChange = () => {
    refreshOptions()
  }

  onMounted(async () => {
    getDatasourceTypes()
    await nextTick()
    refreshOptions()
  })
  return [
    {
      type: 'select',
      field: params.typeField || 'type',
      span: params.span || 12,
      name: t('project.node.datasource_type'),
      props: {
        'on-update:value': onChange
      },
      options: options,
      validate: {
        trigger: ['input', 'blur'],
        required: true
      }
    },
    {
      type: 'select',
      field: params.sourceField || 'datasource',
      span: params.span || 12,
      name: t('project.node.datasource_instances'),
      options: datasourceOptions,
      validate: {
        trigger: ['input', 'blur'],
        required: true,
        validator(unuse: any, value) {
          if (!value && value !== 0) {
            return Error(t('project.node.datasource_instances'))
          }
        }
      }
    }
  ]
}
