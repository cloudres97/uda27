package com.uda.spec.entity;

public enum EPluginParaSparkEnum {
    Executor_count(0),
    Executor_cores(1),
    Executor_memoryMB(2),
    Driver_cores(3),
    Driver_memoryMB(4);

    private final int code;

    EPluginParaSparkEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static EPluginParaSparkEnum fromCode(int code) {
        for (EPluginParaSparkEnum value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported EPluginParaSparkEnum code: " + code);
    }
}
