package com.uda.spec;

import com.uda.spec.entity.PluginSpecEntity;
import com.uda.spec.entity.PluginSpecSparkEntity;
import com.uda.spec.flink.FlinkSpecModifier;
import com.uda.spec.spark.SparkSpecModifier;

public class TaskSpecModifyService {
    private final FlinkSpecModifier flinkSpecModifier;
    private final SparkSpecModifier sparkSpecModifier;

    public TaskSpecModifyService(SparkSpecModifier sparkSpecModifier) {
        this(null, sparkSpecModifier);
    }

    public TaskSpecModifyService(FlinkSpecModifier flinkSpecModifier, SparkSpecModifier sparkSpecModifier) {
        this.flinkSpecModifier = flinkSpecModifier;
        this.sparkSpecModifier = sparkSpecModifier;
    }

    public void modifyFlinkSpec(PluginSpecEntity specEntity) {
        if (flinkSpecModifier == null) {
            throw new IllegalStateException("FlinkSpecModifier cannot be null");
        }
        flinkSpecModifier.modify(specEntity);
    }

    public void deleteFlinkSpec(String pluginName) {
        if (flinkSpecModifier == null) {
            throw new IllegalStateException("FlinkSpecModifier cannot be null");
        }
        flinkSpecModifier.delete(pluginName);
    }

    public void modifySparkSpec(PluginSpecSparkEntity specEntity) {
        sparkSpecModifier.modify(specEntity);
    }

    public void deleteSparkSpec(String pluginName) {
        sparkSpecModifier.delete(pluginName);
    }
}
