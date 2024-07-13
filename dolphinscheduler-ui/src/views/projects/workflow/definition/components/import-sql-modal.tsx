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

import { defineComponent, getCurrentInstance, onMounted, PropType, ref, toRefs } from "vue";
import { useI18n } from 'vue-i18n'
import Modal from '@/components/modal'
import { useForm } from './use-form'
import { useModal } from './use-modal'
import { NForm, NFormItem, NButton, NUpload, NIcon, NInput, NSelect } from 'naive-ui'
import { CloudUploadOutlined } from '@vicons/antd'
import { sqlParsePluginList } from "@/service/modules/process-definition";

const props = {
  show: {
    type: Boolean as PropType<boolean>,
    default: false
  }
}

export default defineComponent({
  name: 'sqlWorkflowDefinitionImport',
  props,
  emits: ['update:show', 'update:row', 'updateList'],
  setup(props, ctx) {
    const { importState } = useForm()
    const { handleSqlImportDefinition } = useModal(importState, ctx)
    const hideModal = () => {
      ctx.emit('update:show')
    }

    const options = ref([] as { label: string; value: string }[])
    const loading = ref(false)

    const getPluginProcessList = async () => {
      if (loading.value) return
      loading.value = true
      const res = await sqlParsePluginList()
      options.value = res.map((option: { name: string }) => ({
        label: option.name,
        value: option.name
      }))
      loading.value = false
    }

    onMounted(() => {
      getPluginProcessList()
    })

    const parseType = ref<string>('')

    function handleSelectChange(selectedValue: string) {
      parseType.value = selectedValue
    }

    const handleImport = () => {
      handleSqlImportDefinition()
    }

    const customRequest = ({ file }: any) => {
      importState.importForm.name = file.name
      importState.importForm.file = file.file
      importState.importForm.parseType = parseType.value
    }
    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim

    return {
      hideModal,
      handleImport,
      customRequest,
      ...toRefs(importState),
      trim,
      parseType,
      options,
      loading,
      handleSelectChange,
    }
  },

  render() {
    const { t } = useI18n()

    return (
      <Modal
        show={this.$props.show}
        title={t('project.workflow.upload')}
        onCancel={this.hideModal}
        onConfirm={this.handleImport}
        confirmLoading={this.saving}
      >
        <NForm rules={this.importRules} ref='importFormRef'>
          <NFormItem label={t('project.workflow.upload_file')} path='file'>
            <NButton>
              <NUpload
                v-model={[this.importForm.file, 'value']}
                customRequest={this.customRequest}
                showFileList={false}
              >
                <NButton text>
                  {t('project.workflow.upload')}
                  <NIcon>
                    <CloudUploadOutlined />
                  </NIcon>
                </NButton>
              </NUpload>
            </NButton>
          </NFormItem>
          <NFormItem label={t('project.workflow.file_name')} path='name'>
            <NInput
              allowInput={this.trim}
              v-model={[this.importForm.name, 'value']}
              placeholder={''}
              disabled
            />
          </NFormItem>
          <NFormItem label="选择类型" path='parseType'>
            <NSelect
                v-model={this.parseType}
                onUpdateValue={this.handleSelectChange}
                options={this.options}
                loading={this.loading}
            />
          </NFormItem>
        </NForm>
      </Modal>
    )
  }
})
