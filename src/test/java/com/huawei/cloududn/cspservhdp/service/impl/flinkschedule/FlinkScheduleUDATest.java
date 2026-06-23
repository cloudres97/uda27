package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * FlinkScheduleUDA 单元测试。
 */
@RunWith(MockitoJUnitRunner.class)
public class FlinkScheduleUDATest {

    @Mock
    private FlinkHdfsMetaService hdfsMetaService;

    @Mock
    private FlinkYarnTaskService yarnTaskService;

    @Mock
    private FlinkTaskSubmitService taskSubmitService;

    private FlinkScheduleUDA flinkScheduleUDA;

    private FlinkModelPathHolder modelPathHolder;

    @Before
    public void setUp() {
        modelPathHolder = new FlinkModelPathHolder();
        flinkScheduleUDA = new FlinkScheduleUDA(
                hdfsMetaService, yarnTaskService, taskSubmitService, modelPathHolder);
    }

    @Test
    public void init_shouldSkipWhenHdfsUnavailable() {
        when(hdfsMetaService.probeConnection()).thenReturn(false);

        flinkScheduleUDA.init();

        verify(yarnTaskService, never()).initYarnClient();
        verify(hdfsMetaService, never()).uploadAllLocalMeta();
        verify(hdfsMetaService, never()).listApplicationNames();
    }

    @Test
    public void init_shouldUploadAndCheckTasksWhenHdfsAvailable() {
        when(hdfsMetaService.probeConnection()).thenReturn(true);
        when(hdfsMetaService.listApplicationNames()).thenReturn(Arrays.asList("app-a", "app-b"));
        when(yarnTaskService.isTaskActive("app-a")).thenReturn(true);
        when(yarnTaskService.isTaskActive("app-b")).thenReturn(false);

        flinkScheduleUDA.init();

        verify(yarnTaskService).initYarnClient();
        verify(hdfsMetaService).uploadAllLocalMeta();
        verify(taskSubmitService).submitTask("app-b");
        verify(taskSubmitService, never()).submitTask("app-a");
    }

    @Test
    public void scheduledUploadLocalMeta_shouldSkipWhenHdfsUnavailable() {
        when(hdfsMetaService.probeConnection()).thenReturn(false);

        flinkScheduleUDA.scheduledUploadLocalMeta();

        verify(hdfsMetaService, never()).uploadAllLocalMeta();
    }

    @Test
    public void scheduledUploadLocalMeta_shouldUploadWhenHdfsAvailable() {
        when(hdfsMetaService.probeConnection()).thenReturn(true);

        flinkScheduleUDA.scheduledUploadLocalMeta();

        verify(hdfsMetaService).uploadAllLocalMeta();
    }

    @Test
    public void scheduledCheckFlinkTasks_shouldSubmitInactiveTasks() {
        when(hdfsMetaService.probeConnection()).thenReturn(true);
        when(hdfsMetaService.listApplicationNames()).thenReturn(Collections.singletonList("order-sync"));
        when(yarnTaskService.isTaskActive("order-sync")).thenReturn(false);

        flinkScheduleUDA.scheduledCheckFlinkTasks();

        verify(taskSubmitService).submitTask("order-sync");
    }

    @Test
    public void scheduledCheckFlinkTasks_shouldNotSubmitActiveTasks() {
        when(hdfsMetaService.probeConnection()).thenReturn(true);
        when(hdfsMetaService.listApplicationNames()).thenReturn(Collections.singletonList("order-sync"));
        when(yarnTaskService.isTaskActive("order-sync")).thenReturn(true);

        flinkScheduleUDA.scheduledCheckFlinkTasks();

        verify(taskSubmitService, never()).submitTask("order-sync");
    }

    @Test
    public void scheduledCheckFlinkTasks_shouldSubmitAfterInitAndScheduledCheck() {
        when(hdfsMetaService.probeConnection()).thenReturn(true);
        when(hdfsMetaService.listApplicationNames()).thenReturn(Collections.singletonList("user-event"));
        when(yarnTaskService.isTaskActive("user-event")).thenReturn(false);

        flinkScheduleUDA.init();
        flinkScheduleUDA.scheduledCheckFlinkTasks();

        verify(taskSubmitService, org.mockito.Mockito.times(2)).submitTask("user-event");
    }

    @Test
    public void onMetaModelChangedEvent_shouldUpdateModelPathAndUploadWhenHdfsAvailable() {
        when(hdfsMetaService.probeConnection()).thenReturn(true);

        flinkScheduleUDA.onMetaModelChangedEvent("/data/new-model");

        assertEquals("/data/new-model/", modelPathHolder.getModelPath());
        verify(hdfsMetaService).uploadAllLocalMeta();
    }

    @Test
    public void onMetaModelChangedEvent_shouldSkipUploadWhenHdfsUnavailable() {
        when(hdfsMetaService.probeConnection()).thenReturn(false);

        flinkScheduleUDA.onMetaModelChangedEvent("/data/new-model");

        assertEquals("/data/new-model/", modelPathHolder.getModelPath());
        verify(hdfsMetaService, never()).uploadAllLocalMeta();
    }
}
