package com.uda.spec.flink;

import com.uda.spec.entity.EPluginParaEnum;
import com.uda.spec.exception.SpecModifyException;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

enum FlinkSpecParameter {
    JOB_MANAGER_MEMORY(
            EPluginParaEnum.JobManager_memoryMB,
            Arrays.asList("flink", "resources", "jobmanager", "memory", "process-size"),
            ValueType.MEMORY_MB
    ),
    JOB_MANAGER_CPU(
            EPluginParaEnum.JobManager_cpuCores,
            Arrays.asList("flink", "resources", "jobmanager", "cpu"),
            ValueType.NUMBER_OR_STRING
    ),
    TASK_MANAGER_COUNT(
            EPluginParaEnum.TaskManager_count,
            Arrays.asList("flink", "resources", "taskmanager", "count"),
            ValueType.INTEGER_OR_STRING
    ),
    TASK_MANAGER_MEMORY(
            EPluginParaEnum.TaskManager_memoryMB,
            Arrays.asList("flink", "resources", "taskmanager", "memory", "process-size"),
            ValueType.MEMORY_MB
    ),
    TASK_MANAGER_CPU(
            EPluginParaEnum.TaskManager_cpuCores,
            Arrays.asList("flink", "resources", "taskmanager", "cpu"),
            ValueType.NUMBER_OR_STRING
    ),
    PARALLELISM(
            EPluginParaEnum.Parallelism,
            Arrays.asList("flink", "parallelism", "default"),
            ValueType.INTEGER_OR_STRING
    );

    private final EPluginParaEnum pluginPara;
    private final List<String> yamlPath;
    private final ValueType valueType;

    FlinkSpecParameter(EPluginParaEnum pluginPara, List<String> yamlPath, ValueType valueType) {
        this.pluginPara = pluginPara;
        this.yamlPath = yamlPath;
        this.valueType = valueType;
    }

    static FlinkSpecParameter from(EPluginParaEnum pluginPara) {
        for (FlinkSpecParameter parameter : values()) {
            if (parameter.pluginPara == pluginPara) {
                return parameter;
            }
        }
        throw new SpecModifyException("Unsupported flink spec parameter: " + pluginPara);
    }

    List<String> yamlPath() {
        return yamlPath;
    }

    Object convertValue(String pluginValue) {
        return valueType.convert(pluginValue);
    }

    enum ValueType {
        MEMORY_MB {
            @Override
            Object convert(String value) {
                String trimmed = trim(value);
                return isPlainNumber(trimmed) ? trimmed + "m" : trimmed;
            }
        },
        INTEGER_OR_STRING {
            @Override
            Object convert(String value) {
                String trimmed = trim(value);
                try {
                    return Integer.valueOf(trimmed);
                } catch (NumberFormatException ignored) {
                    return trimmed;
                }
            }
        },
        NUMBER_OR_STRING {
            @Override
            Object convert(String value) {
                String trimmed = trim(value);
                try {
                    BigDecimal number = new BigDecimal(trimmed);
                    return number.scale() <= 0 ? number.intValue() : number.doubleValue();
                } catch (NumberFormatException ignored) {
                    return trimmed;
                }
            }
        };

        abstract Object convert(String value);

        static String trim(String value) {
            return value == null ? "" : value.trim();
        }

        static boolean isPlainNumber(String value) {
            if (value.isEmpty()) {
                return false;
            }
            for (int i = 0; i < value.length(); i++) {
                if (!Character.isDigit(value.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
    }
}
