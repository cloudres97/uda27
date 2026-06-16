package com.uda.spec.flink;

import com.uda.spec.entity.PluginSpecEntity;
import com.uda.spec.exception.SpecModifyException;
import com.uda.spec.restart.ApplicationRestarter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FlinkSpecModifier {
    public static final String DEFAULT_HDFS_ROOT = "hdfs://hacluster/UDA";
    private static final String CONFIG_FILE_NAME = "config.yaml";
    private static final String BACKUP_SUFFIX = ".uda-original";

    private final String hdfsRoot;
    private final Configuration hadoopConfiguration;
    private final ApplicationRestarter restarter;

    public FlinkSpecModifier(ApplicationRestarter restarter) {
        this(DEFAULT_HDFS_ROOT, new Configuration(), restarter);
    }

    public FlinkSpecModifier(String hdfsRoot, Configuration hadoopConfiguration, ApplicationRestarter restarter) {
        this.hdfsRoot = trimTrailingSlash(hdfsRoot);
        this.hadoopConfiguration = hadoopConfiguration;
        this.restarter = restarter;
    }

    public void modify(PluginSpecEntity specEntity) {
        specEntity.validate();
        validateRestarter();

        try (FileSystem fileSystem = FileSystem.get(URI.create(hdfsRoot), hadoopConfiguration)) {
            Path configFile = configPath(specEntity.getPluginName());
            if (!fileSystem.exists(configFile)) {
                throw new SpecModifyException("No flink config file found for plugin: " + specEntity.getPluginName());
            }

            modifyYamlConfig(fileSystem, configFile, specEntity);
            restarter.restartAppliction(specEntity.getPluginName());
        } catch (IOException e) {
            throw new SpecModifyException("Failed to modify flink spec for plugin: " + specEntity.getPluginName(), e);
        }
    }

    public void delete(String pluginName) {
        requireText(pluginName, "pluginName");
        validateRestarter();

        try (FileSystem fileSystem = FileSystem.get(URI.create(hdfsRoot), hadoopConfiguration)) {
            Path backupFile = backupPathFor(configPath(pluginName));
            if (!fileSystem.exists(backupFile)) {
                throw new SpecModifyException("No flink spec backup found for plugin: " + pluginName);
            }

            restoreBackupAtomically(fileSystem, backupFile);
            restarter.restartAppliction(pluginName);
        } catch (IOException e) {
            throw new SpecModifyException("Failed to restore flink spec for plugin: " + pluginName, e);
        }
    }

    private void modifyYamlConfig(FileSystem fileSystem, Path configFile, PluginSpecEntity specEntity) throws IOException {
        Map<String, Object> root = readYamlAsMap(fileSystem, configFile);

        FlinkSpecParameter parameter = FlinkSpecParameter.from(specEntity.getPluginPara());
        ensureOriginalBackup(fileSystem, configFile);
        putValue(root, parameter.yamlPath(), parameter.convertValue(specEntity.getPluginValue()));
        writeYamlAtomically(fileSystem, configFile, root);
    }

    @SuppressWarnings("unchecked")
    private void putValue(Map<String, Object> root, List<String> yamlPath, Object yamlValue) {
        if (yamlPath == null || yamlPath.isEmpty()) {
            throw new SpecModifyException("Flink yaml path cannot be empty");
        }

        Map<String, Object> current = root;
        for (int i = 0; i < yamlPath.size() - 1; i++) {
            String key = yamlPath.get(i);
            Object next = current.get(key);
            if (next == null) {
                Map<String, Object> child = new LinkedHashMap<>();
                current.put(key, child);
                current = child;
                continue;
            }
            if (!(next instanceof Map)) {
                throw new SpecModifyException("YAML node must be a map: " + key);
            }
            current = (Map<String, Object>) next;
        }

        current.put(yamlPath.get(yamlPath.size() - 1), yamlValue);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readYamlAsMap(FileSystem fileSystem, Path configFile) throws IOException {
        Yaml yaml = createYaml();
        try (FSDataInputStream inputStream = fileSystem.open(configFile)) {
            Object loaded = yaml.load(inputStream);
            if (loaded == null) {
                return new LinkedHashMap<>();
            }
            if (!(loaded instanceof Map)) {
                throw new SpecModifyException("YAML root must be a map: " + configFile);
            }
            return (Map<String, Object>) loaded;
        }
    }

    private void writeYamlAtomically(FileSystem fileSystem, Path targetFile, Map<String, Object> yamlRoot) throws IOException {
        Path tempFile = new Path(targetFile.getParent(), "." + targetFile.getName() + ".tmp-" + UUID.randomUUID());
        try {
            try (FSDataOutputStream outputStream = fileSystem.create(tempFile, false);
                 Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
                createYaml().dump(yamlRoot, writer);
            }

            if (!fileSystem.delete(targetFile, false)) {
                throw new SpecModifyException("Failed to replace original config file: " + targetFile);
            }
            if (!fileSystem.rename(tempFile, targetFile)) {
                throw new SpecModifyException("Failed to rename temp config file to target: " + targetFile);
            }
        } finally {
            if (fileSystem.exists(tempFile)) {
                fileSystem.delete(tempFile, false);
            }
        }
    }

    private void ensureOriginalBackup(FileSystem fileSystem, Path targetFile) throws IOException {
        Path backupFile = backupPathFor(targetFile);
        if (fileSystem.exists(backupFile)) {
            return;
        }

        Path tempBackupFile = new Path(targetFile.getParent(), "." + targetFile.getName() + ".backup-tmp-" + UUID.randomUUID());
        try {
            copy(fileSystem, targetFile, tempBackupFile);
            if (!fileSystem.rename(tempBackupFile, backupFile) && !fileSystem.exists(backupFile)) {
                throw new SpecModifyException("Failed to create original config backup: " + backupFile);
            }
        } finally {
            if (fileSystem.exists(tempBackupFile)) {
                fileSystem.delete(tempBackupFile, false);
            }
        }
    }

    private void restoreBackupAtomically(FileSystem fileSystem, Path backupFile) throws IOException {
        Path targetFile = targetPathForBackup(backupFile);
        Path tempCurrentFile = new Path(targetFile.getParent(), "." + targetFile.getName() + ".restore-current-" + UUID.randomUUID());
        boolean currentMoved = false;
        boolean restored = false;

        try {
            if (fileSystem.exists(targetFile)) {
                if (!fileSystem.rename(targetFile, tempCurrentFile)) {
                    throw new SpecModifyException("Failed to move current config before restore: " + targetFile);
                }
                currentMoved = true;
            }

            if (!fileSystem.rename(backupFile, targetFile)) {
                rollbackRestore(fileSystem, targetFile, tempCurrentFile, currentMoved);
                throw new SpecModifyException("Failed to restore original config from backup: " + backupFile);
            }

            restored = true;
            if (currentMoved && fileSystem.exists(tempCurrentFile)) {
                fileSystem.delete(tempCurrentFile, false);
            }
        } finally {
            if (!restored) {
                rollbackRestore(fileSystem, targetFile, tempCurrentFile, currentMoved);
            }
            if (fileSystem.exists(tempCurrentFile)) {
                fileSystem.delete(tempCurrentFile, false);
            }
        }
    }

    private void rollbackRestore(FileSystem fileSystem, Path targetFile, Path tempCurrentFile, boolean currentMoved) throws IOException {
        if (currentMoved && !fileSystem.exists(targetFile) && fileSystem.exists(tempCurrentFile)) {
            fileSystem.rename(tempCurrentFile, targetFile);
        }
    }

    private void copy(FileSystem fileSystem, Path sourceFile, Path targetFile) throws IOException {
        try (FSDataInputStream inputStream = fileSystem.open(sourceFile);
             FSDataOutputStream outputStream = fileSystem.create(targetFile, false)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) >= 0) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
    }

    private Path configPath(String pluginName) {
        return new Path(hdfsRoot + "/" + pluginName + "/" + CONFIG_FILE_NAME);
    }

    private Path backupPathFor(Path targetFile) {
        return new Path(targetFile.getParent(), backupFileName(targetFile.getName()));
    }

    private String backupFileName(String targetFileName) {
        return "." + targetFileName + BACKUP_SUFFIX;
    }

    private Path targetPathForBackup(Path backupFile) {
        String backupFileName = backupFile.getName();
        if (!backupFileName.startsWith(".") || !backupFileName.endsWith(BACKUP_SUFFIX)) {
            throw new SpecModifyException("Invalid flink spec backup file name: " + backupFile);
        }
        String targetFileName = backupFileName.substring(1, backupFileName.length() - BACKUP_SUFFIX.length());
        return new Path(backupFile.getParent(), targetFileName);
    }

    private Yaml createYaml() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        options.setIndent(2);
        return new Yaml(options);
    }

    private static String trimTrailingSlash(String value) {
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT_HDFS_ROOT;
        }
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private void validateRestarter() {
        if (restarter == null) {
            throw new IllegalStateException("ApplicationRestarter cannot be null");
        }
    }

    private void requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
