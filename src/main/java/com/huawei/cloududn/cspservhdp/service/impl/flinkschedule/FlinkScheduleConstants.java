package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.EnumSet;

/**
 * Flink 调度模块常量定义。
 */
public final class FlinkScheduleConstants {

    /** 本地 Flink 任务元数据根目录，每个子目录对应一个 applicationName。 */
    public static final String LOCAL_META_DIR = "/opt/cloududn/App/meta/flink";

    /** HDFS 上 Flink 插件任务元数据根目录。 */
    public static final String HDFS_FLINK_ROOT = "hdfs://hacluster/uda/plugin/flink";

    /** Flink 任务插件 JAR 路径。 */
    public static final String FLINK_JAR_PATH = "/opt/cloududn/App/lib/task-plugin-flink.jar";

    /** Flink Application 模式主类。 */
    public static final String FLINK_MAIN_CLASS = "com.taskplugin.flink.FlinkTaskApplication";

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
}
