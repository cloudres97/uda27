package com.uda.spec.entity;

import java.util.Objects;

public class PluginSpecSparkEntity {
    private String pluginName;
    private EPluginParaSparkEnum pluginPara;
    private String pluginValue;

    public PluginSpecSparkEntity() {
    }

    public PluginSpecSparkEntity(String pluginName, EPluginParaSparkEnum pluginPara, String pluginValue) {
        this.pluginName = pluginName;
        this.pluginPara = pluginPara;
        this.pluginValue = pluginValue;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public EPluginParaSparkEnum getPluginPara() {
        return pluginPara;
    }

    public void setPluginPara(EPluginParaSparkEnum pluginPara) {
        this.pluginPara = pluginPara;
    }

    public String getPluginValue() {
        return pluginValue;
    }

    public void setPluginValue(String pluginValue) {
        this.pluginValue = pluginValue;
    }

    public void validate() {
        requireText(pluginName, "pluginName");
        requireNonNull(pluginPara, "pluginPara");
        requireText(pluginValue, "pluginValue");
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
    }

    private static void requireText(String value, String fieldName) {
        if (Objects.isNull(value) || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
