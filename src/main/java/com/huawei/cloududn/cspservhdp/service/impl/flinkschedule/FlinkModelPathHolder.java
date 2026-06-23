package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Flink 调度模块 modelPath 运行时上下文。
 * <p>
 * 由 {@link FlinkScheduleUDA#onMetaModelChangedEvent(String)} 更新，
 * 供 {@link FlinkHdfsMetaService} 等组件读取当前本地元数据根路径。
 * </p>
 */
@Component
public class FlinkModelPathHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkModelPathHolder.class);

    private final AtomicReference<String> modelPath =
            new AtomicReference<>(FlinkScheduleConstants.DEFAULT_MODEL_PATH);

    /**
     * 获取当前 modelPath。
     *
     * @return 模型根路径
     */
    public String getModelPath() {
        return modelPath.get();
    }

    /**
     * 获取当前本地 Flink 元数据根目录。
     *
     * @return modelPath + model/compute/FlinkSQLJob/
     */
    public String getLocalMetaDir() {
        return FlinkScheduleConstants.resolveLocalMetaDir(modelPath.get());
    }

    /**
     * 更新 modelPath。
     *
     * @param modelPath 外部传入的模型根路径
     */
    public void updateModelPath(String modelPath) {
        if (modelPath == null || modelPath.isEmpty()) {
            LOGGER.warn("Ignore invalid modelPath: {}", modelPath);
            return;
        }
        String normalized = FlinkScheduleConstants.normalizeModelPath(modelPath);
        this.modelPath.set(normalized);
        LOGGER.info("Model path updated to {}", normalized);
    }
}
