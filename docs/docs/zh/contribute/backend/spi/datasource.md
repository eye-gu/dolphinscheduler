## DolphinScheduler Datasource SPI 主要设计

#### 如何使用数据源？

数据源中心默认支持POSTGRESQL、HIVE/IMPALA、SPARK、CLICKHOUSE、SQLSERVER数据源。

如果使用的是MySQL、ORACLE数据源则需要、把对应的驱动包放置lib目录下

#### 如何进行数据源插件开发？

org.apache.dolphinscheduler.spi.datasource.DataSourceChannel
org.apache.dolphinscheduler.spi.datasource.DataSourceChannelFactory
org.apache.dolphinscheduler.spi.datasource.client.DataSourceClient

1. 第一步数据源插件实现以上接口和继承通用client即可，具体可以参考sqlserver、mysql等数据源插件实现，所有RDBMS插件的添加方式都是一样的。
2. 在数据源插件pom.xml添加驱动配置

我们在 dolphinscheduler-datasource-api 模块提供了所有数据源对外访问的 API

另外，DataSourceChannelFactory 继承自PrioritySPI，这意味着你可以设置插件的优先级，当你有两个插件同名时，你可以通过重写getIdentify 方法来自定义优先级。高优先级的插件会被加载，但是如果你有两个同名且优先级相同的插件，加载插件时服务器会抛出 `IllegalArgumentException`。

#### 如何开发自定义数据源插件（DSIP-110）？

数据源类型由插件声明的**唯一、大小写归一（大写）的字符串名称**标识——不需要存在于 `DbType` 枚举中。
因此在 DolphinScheduler 源码树之外编译的插件可以引入全新的数据源类型，而无需修改任何 DolphinScheduler 代码。

一个自定义插件是一个 shaded jar，其中打包：

1. `DataSourceProcessor` 实现。`getType()` 返回唯一的类型名（如 `MY_INTERNAL_DB`），可选的元数据方法驱动
   UI 行为：`getLabel()`（展示名称）、`getDefaultPort()`（通用表单的默认端口建议）、`isJdbcCompatible()`
   （是否适用通用 JDBC 表单）。
2. `DataSourceChannelFactory` 实现（以及 `DataSourceChannel` 和 ad-hoc/池化 client，通常继承
   `BaseAdHocDataSourceClient` / `BasePooledDataSourceClient`）。
3. `META-INF/services` 注册（与内置插件一样使用 `@AutoService` 生成）。
4. 插件所需的 JDBC 驱动（与内置插件 pom 相同的 shade 方式）。

`dolphinscheduler-datasource-h2` 模块是参考实现，可以直接复制作为起点。

安装 jar 之后，新类型即可端到端使用：

- 类型通过 `GET /datasources/types` 自动出现在 UI 中（数据源中心和任务节点的数据源选择器）；前端没有
  专用表单的类型会渲染为通用 JDBC 表单（主机/端口/用户名/密码/数据库/额外 JDBC 参数）。
- 可以创建、测试连接、更新、列出、授权该类型的数据源实例，并且（SQL / 存储过程）任务可以基于它运行。

运维注意事项：

- 需要将插件 jar 安装到**所有**会接触该类型的 api-server、master 和 worker 节点的 `datasource-plugins`
  目录并重启服务。插件安装是部署动作；普通平台用户只能使用已安装的类型。
- 两个插件声明相同类型名且优先级相同时，服务启动会快速且明确地失败。若要故意覆盖内置类型，请注册一个
  同名且优先级更高的 `DataSourceChannelFactory`。
- 插件之间的驱动版本冲突是平坦类模型的已知限制：每个类路径上同一驱动请只保留一个大版本。
- 在缺少插件的节点上，该类型的数据源仍可被列出（标记为不可用），连接/运行会以明确的错误
  `datasource type 'X' is not installed` 失败。

#### **未来计划**

支持kafka、http、文件、sparkSQL、FlinkSQL等数据源
