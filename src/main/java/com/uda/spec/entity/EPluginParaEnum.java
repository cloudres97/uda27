package com.uda.spec.entity;

public enum EPluginParaEnum {
    JobManager_memoryMB(0),
    JobManager_cpuCores(1),
    TaskManager_count(2),
    TaskManager_memoryMB(3),
    TaskManager_cpuCores(4),
    Parallelism(5);

    private final int code;

    EPluginParaEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static EPluginParaEnum fromCode(int code) {
        for (EPluginParaEnum value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported EPluginParaEnum code: " + code);
    }
}
