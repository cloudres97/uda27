package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * FlinkYarnTaskService 单元测试。
 */
@RunWith(MockitoJUnitRunner.class)
public class FlinkYarnTaskServiceTest {

    @Mock
    private YarnClient yarnClient;

    private FlinkYarnTaskService yarnTaskService;

    @Before
    public void setUp() {
        yarnTaskService = new FlinkYarnTaskService();
        yarnTaskService.setYarnClientForTest(yarnClient);
    }

    @Test
    public void isTaskActive_shouldReturnTrueWhenApplicationIsRunning() throws Exception {
        ApplicationReport report = mock(ApplicationReport.class);
        when(report.getName()).thenReturn("order-sync");
        when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.RUNNING);
        when(report.getApplicationId()).thenReturn(ApplicationId.newInstance(1L, 1));
        when(yarnClient.getApplications(eq(FlinkScheduleConstants.ACTIVE_YARN_STATES)))
                .thenReturn(Collections.singletonList(report));

        assertTrue(yarnTaskService.isTaskActive("order-sync"));
    }

    @Test
    public void isTaskActive_shouldReturnTrueWhenApplicationIsSubmitted() throws Exception {
        ApplicationReport report = mock(ApplicationReport.class);
        when(report.getName()).thenReturn("user-event");
        when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.SUBMITTED);
        when(report.getApplicationId()).thenReturn(ApplicationId.newInstance(1L, 2));
        when(yarnClient.getApplications(eq(FlinkScheduleConstants.ACTIVE_YARN_STATES)))
                .thenReturn(Collections.singletonList(report));

        assertTrue(yarnTaskService.isTaskActive("user-event"));
    }

    @Test
    public void isTaskActive_shouldReturnFalseWhenApplicationNotFound() throws Exception {
        ApplicationReport report = mock(ApplicationReport.class);
        when(report.getName()).thenReturn("other-app");
        when(yarnClient.getApplications(eq(FlinkScheduleConstants.ACTIVE_YARN_STATES)))
                .thenReturn(Collections.singletonList(report));

        assertFalse(yarnTaskService.isTaskActive("order-sync"));
    }

    @Test
    public void isTaskActive_shouldReturnFalseWhenYarnQueryFails() throws Exception {
        when(yarnClient.getApplications(eq(FlinkScheduleConstants.ACTIVE_YARN_STATES)))
                .thenThrow(new RuntimeException("YARN unavailable"));

        assertFalse(yarnTaskService.isTaskActive("order-sync"));
    }

    @Test
    public void isTaskActive_shouldReturnFalseWhenNoActiveApplications() throws Exception {
        when(yarnClient.getApplications(eq(FlinkScheduleConstants.ACTIVE_YARN_STATES)))
                .thenReturn(Collections.emptyList());

        assertFalse(yarnTaskService.isTaskActive("order-sync"));
    }

    @Test
    public void isTaskActive_shouldMatchByExactApplicationName() throws Exception {
        ApplicationReport report1 = mock(ApplicationReport.class);
        when(report1.getName()).thenReturn("app-a");

        ApplicationReport report2 = mock(ApplicationReport.class);
        when(report2.getName()).thenReturn("app-b");
        when(report2.getYarnApplicationState()).thenReturn(YarnApplicationState.ACCEPTED);
        when(report2.getApplicationId()).thenReturn(ApplicationId.newInstance(1L, 4));

        when(yarnClient.getApplications(eq(FlinkScheduleConstants.ACTIVE_YARN_STATES)))
                .thenReturn(Arrays.asList(report1, report2));

        assertTrue(yarnTaskService.isTaskActive("app-b"));
        assertFalse(yarnTaskService.isTaskActive("app-c"));
    }
}
