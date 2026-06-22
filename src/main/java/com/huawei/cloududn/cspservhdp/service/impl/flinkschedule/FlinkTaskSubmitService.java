package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink 任务命令行提交服务。
 * <p>
 * 拼装 flink run-application 命令并通过子进程执行。
 * </p>
 */
@Component
public class FlinkTaskSubmitService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkTaskSubmitService.class);

    @Value("${flink.schedule.flink-bin:flink}")
    private String flinkBin;

    /**
     * 提交指定 application 的 Flink 任务。
     *
     * @param applicationName 应用名称
     * @return true 表示命令执行成功（退出码为 0）
     */
    public boolean submitTask(String applicationName) {
        String taskPath = FlinkScheduleConstants.HDFS_FLINK_ROOT + "/" + applicationName + "/";
        List<String> command = buildSubmitCommand(applicationName, taskPath);
        LOGGER.info("Submitting Flink task {}, command={}", applicationName, command);
        int exitCode = executeCommand(command);
        if (exitCode == 0) {
            LOGGER.info("Flink task {} submitted successfully", applicationName);
            return true;
        }
        LOGGER.error("Flink task {} submit failed, exitCode={}", applicationName, exitCode);
        return false;
    }

    /**
     * 拼装 flink run-application 提交命令。
     *
     * @param applicationName 应用名称
     * @param taskPath        HDFS 任务路径
     * @return 命令参数列表
     */
    List<String> buildSubmitCommand(String applicationName, String taskPath) {
        List<String> command = new ArrayList<>();
        command.add(flinkBin);
        command.add("run-application");
        command.add("-t");
        command.add("yarn-application");
        // 与 HDFS 子目录名保持一致，便于 YARN 按名称查询
        command.add("-Dyarn.application.name=" + applicationName);
        command.add("-c");
        command.add(FlinkScheduleConstants.FLINK_MAIN_CLASS);
        command.add(FlinkScheduleConstants.FLINK_JAR_PATH);
        command.add("--task-path");
        command.add(taskPath);
        return command;
    }

    int executeCommand(List<String> command) {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        try {
            LOGGER.debug("Starting process: {}", command);
            Process process = processBuilder.start();
            logProcessOutput(process);
            int exitCode = process.waitFor();
            LOGGER.debug("Process finished, exitCode={}", exitCode);
            return exitCode;
        } catch (IOException | InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.error("Failed to execute command {}", command, ex);
            return -1;
        }
    }

    private void logProcessOutput(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                LOGGER.info("Flink submit output: {}", line);
            }
        }
    }

    void setFlinkBinForTest(String flinkBin) {
        this.flinkBin = flinkBin;
    }
}
