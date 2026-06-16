package com.uda.spec.spark;

import com.uda.spec.entity.EPluginParaSparkEnum;
import com.uda.spec.exception.SpecModifyException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

enum SparkSpecParameter {
    EXECUTOR_COUNT(
            EPluginParaSparkEnum.Executor_count,
            "spark.executor.instances",
            ValueType.STRING,
            "spark.driver.instance"
    ),
    EXECUTOR_CORES(
            EPluginParaSparkEnum.Executor_cores,
            "spark.executor.cores",
            ValueType.STRING
    ),
    EXECUTOR_MEMORY(
            EPluginParaSparkEnum.Executor_memoryMB,
            "spark.executor.memory",
            ValueType.MEMORY_MB
    ),
    DRIVER_CORES(
            EPluginParaSparkEnum.Driver_cores,
            "spark.driver.cores",
            ValueType.STRING
    ),
    DRIVER_MEMORY(
            EPluginParaSparkEnum.Driver_memoryMB,
            "spark.driver.memory",
            ValueType.MEMORY_MB
    );

    private final EPluginParaSparkEnum pluginPara;
    private final String defaultSparkKey;
    private final ValueType valueType;
    private final List<String> compatibleKeys;

    SparkSpecParameter(EPluginParaSparkEnum pluginPara, String defaultSparkKey, ValueType valueType, String... compatibleKeys) {
        this.pluginPara = pluginPara;
        this.defaultSparkKey = defaultSparkKey;
        this.valueType = valueType;
        this.compatibleKeys = compatibleKeys == null ? Collections.<String>emptyList() : Arrays.asList(compatibleKeys);
    }

    static SparkSpecParameter from(EPluginParaSparkEnum pluginPara) {
        for (SparkSpecParameter parameter : values()) {
            if (parameter.pluginPara == pluginPara) {
                return parameter;
            }
        }
        throw new SpecModifyException("Unsupported spark spec parameter: " + pluginPara);
    }

    String resolveConfigKey(List<String> existingKeys) {
        if (existingKeys.contains(defaultSparkKey)) {
            return defaultSparkKey;
        }
        for (String compatibleKey : compatibleKeys) {
            if (existingKeys.contains(compatibleKey)) {
                return compatibleKey;
            }
        }
        return defaultSparkKey;
    }

    String convertValue(String pluginValue) {
        return valueType.convert(pluginValue);
    }

    enum ValueType {
        STRING {
            @Override
            String convert(String value) {
                return trim(value);
            }
        },
        MEMORY_MB {
            @Override
            String convert(String value) {
                String trimmed = trim(value);
                return isPlainNumber(trimmed) ? trimmed + "m" : trimmed;
            }
        };

        abstract String convert(String value);

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
