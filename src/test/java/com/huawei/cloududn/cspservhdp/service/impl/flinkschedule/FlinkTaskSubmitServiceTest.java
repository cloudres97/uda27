package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * FlinkTaskSubmitService 单元测试。
 */
public class FlinkTaskSubmitServiceTest {

    private FlinkTaskSubmitService submitService;

    @Before
    public void setUp() {
        submitService = new FlinkTaskSubmitService();
        submitService.setFlinkBinForTest("/opt/Bigdata/Flink/flink/bin/flink");
    }

    @Test
    public void buildSubmitCommand_shouldContainAllRequiredArguments() {
        List<String> command = submitService.buildSubmitCommand(
                "order-sync",
                "hdfs://hacluster/uda/plugin/flink/order-sync/");

        assertEquals("/opt/Bigdata/Flink/flink/bin/flink", command.get(0));
        assertEquals("run-application", command.get(1));
        assertEquals("-t", command.get(2));
        assertEquals("yarn-application", command.get(3));
        assertEquals("-c", command.get(4));
        assertEquals(FlinkScheduleConstants.FLINK_MAIN_CLASS, command.get(5));
        assertEquals("-Dyarn.ship-files=" + FlinkScheduleConstants.FLINK_YARN_SHIP_FILES, command.get(6));
        assertEquals("-Denv.java.opts=" + FlinkScheduleConstants.FLINK_ENV_JAVA_OPTS, command.get(7));
        assertEquals(FlinkScheduleConstants.FLINK_JAR_PATH, command.get(8));
        assertEquals("--task-path", command.get(9));
        assertEquals("hdfs://hacluster/uda/plugin/flink/order-sync/", command.get(10));
    }

    @Test
    public void buildSubmitCommand_shouldUseApplicationNameInYarnAndTaskPath() {
        List<String> command = submitService.buildSubmitCommand(
                "user-event",
                "hdfs://hacluster/uda/plugin/flink/user-event/");

        assertTrue(command.contains("hdfs://hacluster/uda/plugin/flink/user-event/"));
    }

    @Test
    public void submitTask_shouldReturnFalseWhenCommandFails() {
        FlinkTaskSubmitService spyService = new FlinkTaskSubmitService() {
            @Override
            int executeCommand(List<String> command) {
                return 1;
            }
        };

        assertFalse(spyService.submitTask("failed-app"));
    }

    @Test
    public void submitTask_shouldReturnTrueWhenCommandSucceeds() {
        FlinkTaskSubmitService spyService = new FlinkTaskSubmitService() {
            @Override
            int executeCommand(List<String> command) {
                return 0;
            }
        };

        assertTrue(spyService.submitTask("success-app"));
    }
}
