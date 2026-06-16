package com.uda.spec.restart;

@FunctionalInterface
public interface ApplicationRestarter {
    void restartAppliction(String pluginName);
}
