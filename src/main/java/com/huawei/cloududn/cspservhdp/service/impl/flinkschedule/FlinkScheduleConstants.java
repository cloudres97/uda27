package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.EnumSet;

/**
 * Flink 调度模块常量定义。
 */
public final class FlinkScheduleConstants {

    /** 模型根路径，由外部输入；当前先在常量中配置默认值。 */
    public static final String MODEL_PATH = "/opt/cloududn/App/metamodel-storage/";

    private static final String LOCAL_META_RELATIVE_PATH = "model/compute/FlinkSQLJob/";

    /** 本地 Flink 任务元数据根目录，每个子目录对应一个 applicationName。 */
    public static final String LOCAL_META_DIR = resolveLocalMetaDir(MODEL_PATH);

    /** HDFS 上 Flink 插件任务元数据根目录。 */
    public static final String HDFS_FLINK_ROOT = "hdfs://hacluster/uda/plugin/flink";

    /** Flink 任务插件 JAR 路径。 */
    public static final String FLINK_JAR_PATH = "/opt/cloududn/App/lib/task-plugin-flink.jar";

    /** Flink Application 模式主类。 */
    public static final String FLINK_MAIN_CLASS = "com.taskplugin.flink.FlinkTaskApplication";

    /** YARN 分发到容器的依赖文件列表，分号分隔。 */
    public static final String FLINK_YARN_SHIP_FILES =
            "/opt/cloududn/App/DAYUClient/Keytab/paas.keytab;"
                    + "/opt/cloududn/App/DAYUClient/Client/Kafka/config/kdc.conf;"
                    + "/opt/container/envinfo/cert/internal/trust.cer;"
                    + "/opt/cloududn/App/DAYUClient/Client/Flink/config/yarn-site.xml";

    /** Flink 容器 JVM 启动参数。 */
    public static final String FLINK_ENV_JAVA_OPTS = "-Djava.security.krb5.conf=kdc.conf";

    /**
     * YARN 上视为“正在拉起或运行中”的状态集合。
     * 处于这些状态时不需要重复提交任务。
     */
    public static final EnumSet<YarnApplicationState> ACTIVE_YARN_STATES = EnumSet.of(
            YarnApplicationState.NEW,
            YarnApplicationState.NEW_SAVING,
            YarnApplicationState.SUBMITTED,
            YarnApplicationState.ACCEPTED,
            YarnApplicationState.RUNNING);

    private FlinkScheduleConstants() {
    }

    /**
     * 根据 modelPath 解析本地 Flink 元数据根目录。
     *
     * @param modelPath 模型根路径
     * @return modelPath + model/compute/FlinkSQLJob/
     */
    public static String resolveLocalMetaDir(String modelPath) {
        return normalizeModelPath(modelPath) + LOCAL_META_RELATIVE_PATH;
    }

    private static String normalizeModelPath(String modelPath) {
        if (modelPath == null || modelPath.isEmpty()) {
            return "";
        }
        return modelPath.endsWith("/") ? modelPath : modelPath + "/";
    }
}
