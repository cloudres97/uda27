package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * FlinkModelPathHolder 单元测试。
 */
public class FlinkModelPathHolderTest {

    @Test
    public void getLocalMetaDir_shouldUseDefaultModelPathInitially() {
        FlinkModelPathHolder holder = new FlinkModelPathHolder();

        assertEquals(FlinkScheduleConstants.DEFAULT_MODEL_PATH, holder.getModelPath());
        assertEquals(
                FlinkScheduleConstants.resolveLocalMetaDir(FlinkScheduleConstants.DEFAULT_MODEL_PATH),
                holder.getLocalMetaDir());
    }

    @Test
    public void updateModelPath_shouldNormalizePathAndRefreshLocalMetaDir() {
        FlinkModelPathHolder holder = new FlinkModelPathHolder();

        holder.updateModelPath("/data/custom-model");

        assertEquals("/data/custom-model/", holder.getModelPath());
        assertEquals("/data/custom-model/model/compute/FlinkSQLJob/", holder.getLocalMetaDir());
    }

    @Test
    public void updateModelPath_shouldIgnoreInvalidValue() {
        FlinkModelPathHolder holder = new FlinkModelPathHolder();

        holder.updateModelPath(null);
        holder.updateModelPath("");

        assertEquals(FlinkScheduleConstants.DEFAULT_MODEL_PATH, holder.getModelPath());
    }
}
