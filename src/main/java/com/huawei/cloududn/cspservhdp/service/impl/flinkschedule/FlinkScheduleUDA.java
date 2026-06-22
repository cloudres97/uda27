package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Flink 任务调度 Bean（UDA）。
 * <p>
 * 核心职责：
 * <ol>
 *   <li>定时将本地 {@code modelPath/model/compute/FlinkSQLJob/} 元数据同步到 HDFS</li>
 *   <li>Bean 加载时探测 HDFS，扫描 HDFS 上各 application 目录并检查 YARN 任务状态</li>
 *   <li>对非运行中的 Flink 任务执行 flink run-application 提交</li>
 *   <li>周期性巡检所有 Flink 任务，异常退出时自动拉起</li>
 * </ol>
 * </p>
 * <p>
 * 需在 Spring 配置类上添加 {@code @EnableScheduling} 以启用定时任务。
 * </p>
 */
@Service
public class FlinkScheduleUDA {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkScheduleUDA.class);

    private final FlinkHdfsMetaService hdfsMetaService;

    private final FlinkYarnTaskService yarnTaskService;

    private final FlinkTaskSubmitService taskSubmitService;

    /** 按 applicationName 维度的提交锁，防止并发重复提交同一任务。 */
    private final Map<String, Lock> applicationLocks = new ConcurrentHashMap<>();

    /** HDFS 是否可用，不可用时会周期性重试探测。 */
    private volatile boolean hdfsAvailable;

    @Autowired
    public FlinkScheduleUDA(FlinkHdfsMetaService hdfsMetaService,
                            FlinkYarnTaskService yarnTaskService,
                            FlinkTaskSubmitService taskSubmitService) {
        this.hdfsMetaService = hdfsMetaService;
        this.yarnTaskService = yarnTaskService;
        this.taskSubmitService = taskSubmitService;
    }

    /**
     * Bean 初始化入口。
     * <p>
     * 流程：探测 HDFS → 初始化 YarnClient → 上传本地 meta → 检查并恢复所有 Flink 任务。
     * </p>
     */
    @PostConstruct
    public void init() {
        LOGGER.info("FlinkScheduleUDA initializing");
        hdfsAvailable = hdfsMetaService.probeConnection();
        if (!hdfsAvailable) {
            LOGGER.warn("HDFS unavailable at startup, skip initial Flink task check and upload");
            return;
        }
        yarnTaskService.initYarnClient();
        hdfsMetaService.uploadAllLocalMeta();
        checkAndRecoverAllFlinkTasks();
        LOGGER.info("FlinkScheduleUDA initialization completed");
    }

    /**
     * 定时将本地 meta 目录同步到 HDFS。
     * <p>默认每 5 分钟执行一次，可通过 {@code flink.schedule.upload-cron} 配置。</p>
     */
    @Scheduled(cron = "${flink.schedule.upload-cron:0 */5 * * * ?}")
    public void scheduledUploadLocalMeta() {
        LOGGER.info("Scheduled local meta upload triggered");
        if (!ensureHdfsReady()) {
            LOGGER.warn("Skip scheduled upload because HDFS is unavailable");
            return;
        }
        hdfsMetaService.uploadAllLocalMeta();
    }

    /**
     * 周期性检查所有 Flink 任务状态，不在运行则拉起。
     * <p>默认每 60 秒执行一次，可通过 {@code flink.schedule.check-interval-ms} 配置。</p>
     */
    @Scheduled(fixedDelayString = "${flink.schedule.check-interval-ms:60000}")
    public void scheduledCheckFlinkTasks() {
        LOGGER.debug("Scheduled Flink task health check triggered");
        if (!ensureHdfsReady()) {
            LOGGER.warn("Skip scheduled health check because HDFS is unavailable");
            return;
        }
        checkAndRecoverAllFlinkTasks();
    }

    /**
     * 确保 HDFS 可用；若之前不可用则重新探测。
     *
     * @return true 表示 HDFS 当前可用
     */
    private boolean ensureHdfsReady() {
        if (hdfsAvailable) {
            return true;
        }
        LOGGER.info("HDFS was unavailable, retrying connection probe");
        hdfsAvailable = hdfsMetaService.probeConnection();
        if (hdfsAvailable) {
            LOGGER.info("HDFS connection recovered");
            yarnTaskService.initYarnClient();
        }
        return hdfsAvailable;
    }

    /**
     * 扫描 HDFS 上所有 application 并逐一检查、恢复。
     */
    private void checkAndRecoverAllFlinkTasks() {
        LOGGER.info("Start checking all Flink tasks under {}", FlinkScheduleConstants.HDFS_FLINK_ROOT);
        List<String> applicationNames = hdfsMetaService.listApplicationNames();
        if (applicationNames.isEmpty()) {
            LOGGER.info("No Flink application found under {}", FlinkScheduleConstants.HDFS_FLINK_ROOT);
            return;
        }
        LOGGER.info("Checking {} Flink application(s): {}", applicationNames.size(), applicationNames);
        for (String applicationName : applicationNames) {
            ensureFlinkTaskRunning(applicationName);
        }
        LOGGER.info("Flink task health check finished");
    }

    /**
     * 确保单个 application 对应的 Flink 任务处于运行状态。
     * <p>
     * 使用 tryLock 避免同一 application 被多个线程同时提交；
     * 若 YARN 上无活跃任务则触发命令行提交。
     * </p>
     *
     * @param applicationName 应用名称
     */
    private void ensureFlinkTaskRunning(String applicationName) {
        Lock lock = applicationLocks.computeIfAbsent(applicationName, key -> new ReentrantLock());
        if (!lock.tryLock()) {
            LOGGER.info("Another thread is handling application {}, skip this round", applicationName);
            return;
        }
        try {
            LOGGER.debug("Checking Flink task status for application={}", applicationName);
            if (yarnTaskService.isTaskActive(applicationName)) {
                LOGGER.debug("Flink task {} is starting or running, no action needed", applicationName);
                return;
            }
            LOGGER.warn("Flink task {} is not active, preparing to submit", applicationName);
            taskSubmitService.submitTask(applicationName);
        } finally {
            lock.unlock();
        }
    }
}
