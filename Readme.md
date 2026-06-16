
# UDA Runtime Spec Modifier

运行中修改 Flink/Spark 任务规格参数的入口。

## Spark

```java
ApplicationRestarter restarter = pluginName -> restartAppliction(pluginName);

FlinkSpecModifier flinkSpecModifier = new FlinkSpecModifier(restarter);
SparkSpecModifier sparkSpecModifier = new SparkSpecModifier(restarter);
TaskSpecModifyService service = new TaskSpecModifyService(flinkSpecModifier, sparkSpecModifier);

service.modifySparkSpec(new PluginSpecSparkEntity("demoPlugin", EPluginParaSparkEnum.Executor_cores, "4"));
service.deleteSparkSpec("demoPlugin");
```

Spark 修改逻辑：

- HDFS 根目录默认是 `hdfs://hacluster/UDA`
- `daily_trigger`：递归查找名为 `PluginName` 的目录，再递归查找 `config.yaml`
- `interval_trigger`：递归查找名为 `PluginName` 的目录，再递归查找 `app-config.yaml`
- `event_trigger`：不处理
- 第一次修改前，会在源文件同目录创建原始配置备份
- 修改成功写回源文件后，调用 `restartAppliction(PluginName)`

Spark 删除/还原逻辑：

- `deleteSparkSpec(String PluginName)` 会查找之前保存的原始配置备份
- 找到备份后，将当前 `config.yaml` / `app-config.yaml` 替换回原始内容
- 还原成功后删除备份文件，并调用 `restartAppliction(PluginName)`
- 如果没有找到备份，说明当前任务没有通过本模块修改过，接口会抛出异常

备份文件命名：

| 源文件 | 备份文件 |
| --- | --- |
| `config.yaml` | `.config.yaml.uda-original` |
| `app-config.yaml` | `.app-config.yaml.uda-original` |

支持的 `EPluginParaSparkEnum`：

| EPluginParaSparkEnum | Spark config key |
| --- | --- |
| `Executor_count` | 优先修改已有的 `spark.executor.instances` 或兼容旧 key `spark.driver.instance` |
| `Executor_cores` | `spark.executor.cores` |
| `Executor_memoryMB` | `spark.executor.memory` |
| `Driver_cores` | `spark.driver.cores` |
| `Driver_memoryMB` | `spark.driver.memory` |

`Executor_memoryMB` / `Driver_memoryMB` 如果传入纯数字，例如 `2048`，会写成 `2048m`。

## Flink

```java
service.modifyFlinkSpec(new PluginSpecEntity("demoPlugin", EPluginParaEnum.Parallelism, "4"));
service.deleteFlinkSpec("demoPlugin");
```

Flink 修改逻辑：

- HDFS 配置文件路径是 `hdfs://hacluster/UDA/{PluginName}/config.yaml`
- 第一次修改前，会在源文件同目录创建原始配置备份 `.config.yaml.uda-original`
- 修改成功写回源文件后，调用 `restartAppliction(PluginName)`

Flink `EPluginParaEnum` 映射：

| EPluginParaEnum | YAML path |
| --- | --- |
| `JobManager_memoryMB` | `flink.resources.jobmanager.memory.process-size` |
| `JobManager_cpuCores` | `flink.resources.jobmanager.cpu` |
| `TaskManager_count` | `flink.resources.taskmanager.count` |
| `TaskManager_memoryMB` | `flink.resources.taskmanager.memory.process-size` |
| `TaskManager_cpuCores` | `flink.resources.taskmanager.cpu` |
| `Parallelism` | `flink.parallelism.default` |

`JobManager_memoryMB` / `TaskManager_memoryMB` 如果传入纯数字，例如 `1024`，会写成 `1024m`。
