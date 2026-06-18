package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.List;

/**
 * Flink 任务 YARN 状态查询服务。
 * <p>
 * 通过 YarnClient 按 applicationName 查询任务是否处于拉起或运行状态。
 * </p>
 */
@Component
public class FlinkYarnTaskService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnTaskService.class);

    private final Configuration hadoopConf = new Configuration();

    private YarnClient yarnClient;

    /**
     * 判断指定 applicationName 的 Flink 任务是否处于活跃状态。
     *
     * @param applicationName YARN 应用名称，与 HDFS 子目录名一致
     * @return true 表示任务正在拉起或运行中
     */
    public boolean isTaskActive(String applicationName) {
        LOG.debug("Querying YARN status for application={}", applicationName);
        initYarnClient();
        try {
            List<ApplicationReport> reports = yarnClient.getApplications(
                    FlinkScheduleConstants.ACTIVE_YARN_STATES);
            LOG.debug("Fetched {} active application(s) from YARN", reports.size());
            for (ApplicationReport report : reports) {
                if (applicationName.equals(report.getName())) {
                    LOG.info("Application {} is active on YARN, state={}, appId={}",
                            applicationName,
                            report.getYarnApplicationState(),
                            report.getApplicationId());
                    return true;
                }
            }
            LOG.info("Application {} is not active on YARN", applicationName);
            return false;
        } catch (Exception ex) {
            LOG.error("Failed to query YARN status for application {}", applicationName, ex);
            return false;
        }
    }

    /**
     * 初始化 YarnClient，重复调用时直接返回。
     */
    public void initYarnClient() {
        if (yarnClient != null) {
            return;
        }
        LOG.info("Initializing YarnClient");
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(hadoopConf);
        yarnClient.start();
        LOG.info("YarnClient started");
    }

    @PreDestroy
    public void destroy() {
        if (yarnClient == null) {
            return;
        }
        LOG.info("Stopping YarnClient");
        yarnClient.stop();
        yarnClient = null;
    }

    /**
     * 供单元测试注入 Mock YarnClient。
     *
     * @param yarnClient 测试用 YarnClient
     */
    void setYarnClientForTest(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }
}
