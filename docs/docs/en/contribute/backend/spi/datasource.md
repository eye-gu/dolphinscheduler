## DolphinScheduler Datasource SPI main design

#### How do I use data sources?

The data source center supports POSTGRESQL, HIVE/IMPALA, SPARK, CLICKHOUSE, SQLSERVER data sources by default.

If you are using MySQL or ORACLE data source, you need to place the corresponding driver package in the lib directory

#### How to do Datasource plugin development?

org.apache.dolphinscheduler.spi.datasource.DataSourceChannel
org.apache.dolphinscheduler.spi.datasource.DataSourceChannelFactory
org.apache.dolphinscheduler.spi.datasource.client.DataSourceClient

1. In the first step, the data source plug-in can implement the above interfaces and inherit the general client. For details, refer to the implementation of data source plug-ins such as sqlserver and mysql. The addition methods of all RDBMS plug-ins are the same.

2. Add the driver configuration in the data source plug-in pom.xml

We provide APIs for external access of all data sources in the dolphin scheduler data source API module

In additional, the `DataSourceChannelFactory` extends from `PrioritySPI`, this means you can set the plugin priority, when you have two plugin has the same name, you can customize the priority by override the `getIdentify` method. The high priority plugin will be load, but if you have two plugin with the same name and same priority, the server will throw `IllegalArgumentException` when load the plugin.

#### How to develop a custom datasource plugin (DSIP-110)?

A datasource type is identified by a **unique, case-normalized (upper case) string name** declared by the plugin —
it does not need to exist in the `DbType` enum. A plugin compiled outside the DolphinScheduler tree can therefore
introduce a brand new datasource type without modifying any DolphinScheduler code.

A custom plugin is a single shaded jar bundling:

1. A `DataSourceProcessor` implementation. `getType()` returns the unique type name (e.g. `MY_INTERNAL_DB`), and the
   optional metadata methods drive the UI: `getLabel()` (display name), `getDefaultPort()` (suggested port for the
   generic form), `isJdbcCompatible()` (whether the generic JDBC form applies).
2. A `DataSourceChannelFactory` implementation (plus `DataSourceChannel` and the ad-hoc/pooled clients, usually by
   extending `BaseAdHocDataSourceClient` / `BasePooledDataSourceClient`).
3. The `META-INF/services` registrations (generate them with `@AutoService`, same as the built-in plugins).
4. The JDBC driver the plugin needs (same shading pattern as the built-in plugin poms).

The `dolphinscheduler-datasource-h2` module is the reference implementation — copy it as the starting point.

Once the jar is installed, the new type is usable end-to-end:

- The type shows up in the UI automatically (datasource center and task datasource selector) through
  `GET /datasources/types`; types without a dedicated frontend form are rendered with a generic JDBC form
  (host / port / user / password / database / extra JDBC parameters).
- Datasource instances can be created, connectivity-tested, updated, listed, authorized, and (SQL / Procedure)
  tasks can run against them.

Operational notes:

- Install the plugin jar into the `datasource-plugins` directory of **every** api-server, master and worker node
  that touches the type, then restart the servers. Plugin installation is a deployment action; ordinary platform
  users only use the installed types.
- Two plugins declaring the same type name with the same priority fail the server startup fast and loudly. To
  deliberately override a built-in type, register a higher-priority `DataSourceChannelFactory` with the same name.
- Driver version conflicts between plugins are a known limitation of the flat classpath: keep one major version
  of each driver per classpath.
- On a node where the plugin is missing, datasources of that type are still listable (shown as unavailable), and
  connect/run attempts fail with the explicit error `datasource type 'X' is not installed`.

#### **Future plan**

Support data sources such as kafka, http, files, sparkSQL, FlinkSQL, etc.
