package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink 元数据 HDFS 读写服务。
 * <p>
 * 负责探测 HDFS 连通性、列举 application 目录、将本地 meta 同步到 HDFS。
 * </p>
 */
@Component
public class FlinkHdfsMetaService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkHdfsMetaService.class);

    private final Configuration hadoopConf = new Configuration();

    /**
     * 探测 HDFS 是否可用。
     *
     * @return true 表示 FileSystem 可正常连接
     */
    public boolean probeConnection() {
        LOGGER.info("Start probing HDFS connection, root={}", FlinkScheduleConstants.HDFS_FLINK_ROOT);
        try (FileSystem fileSystem = createFileSystem()) {
            boolean exists = fileSystem.exists(new Path(FlinkScheduleConstants.HDFS_FLINK_ROOT));
            LOGGER.info("HDFS probe success, root exists={}", exists);
            return true;
        } catch (IOException ex) {
            LOGGER.error("HDFS probe failed, root={}", FlinkScheduleConstants.HDFS_FLINK_ROOT, ex);
            return false;
        }
    }

    /**
     * 列举 HDFS 根目录下所有 application 子目录名称。
     *
     * @return applicationName 列表；失败时返回空列表
     */
    public List<String> listApplicationNames() {
        LOGGER.debug("Listing application names from HDFS root={}", FlinkScheduleConstants.HDFS_FLINK_ROOT);
        try (FileSystem fileSystem = createFileSystem()) {
            Path rootPath = new Path(FlinkScheduleConstants.HDFS_FLINK_ROOT);
            if (!fileSystem.exists(rootPath)) {
                LOGGER.warn("HDFS root path does not exist: {}", FlinkScheduleConstants.HDFS_FLINK_ROOT);
                return Collections.emptyList();
            }
            FileStatus[] statuses = fileSystem.listStatus(rootPath);
            List<String> names = Arrays.stream(statuses)
                    .filter(FileStatus::isDirectory)
                    .map(status -> status.getPath().getName())
                    .collect(Collectors.toList());
            LOGGER.info("Found {} application(s) on HDFS: {}", names.size(), names);
            return names;
        } catch (IOException ex) {
            LOGGER.error("Failed to list applications from HDFS", ex);
            return Collections.emptyList();
        }
    }

    /**
     * 将本地 meta 根目录下所有 application 子目录同步到 HDFS。
     */
    public void uploadAllLocalMeta() {
        LOGGER.info("Start uploading local meta from {}", FlinkScheduleConstants.LOCAL_META_DIR);
        File localRoot = new File(FlinkScheduleConstants.LOCAL_META_DIR);
        if (!localRoot.isDirectory()) {
            LOGGER.warn("Local meta directory not found or not a directory: {}",
                    FlinkScheduleConstants.LOCAL_META_DIR);
            return;
        }
        File[] applicationDirs = localRoot.listFiles(File::isDirectory);
        if (applicationDirs == null || applicationDirs.length == 0) {
            LOGGER.info("No application meta subdirectory under {}", FlinkScheduleConstants.LOCAL_META_DIR);
            return;
        }
        LOGGER.info("Found {} local application meta director(ies) to upload", applicationDirs.length);
        for (File applicationDir : applicationDirs) {
            uploadApplicationMeta(applicationDir);
        }
        LOGGER.info("Local meta upload finished");
    }

    /**
     * 上传单个 application 的本地 meta 到 HDFS 对应目录。
     *
     * @param applicationDir 本地 application 目录
     */
    public void uploadApplicationMeta(File applicationDir) {
        String applicationName = applicationDir.getName();
        Path hdfsAppPath = new Path(FlinkScheduleConstants.HDFS_FLINK_ROOT, applicationName);
        LOGGER.info("Uploading meta for application={}, local={}, hdfs={}",
                applicationName, applicationDir.getAbsolutePath(), hdfsAppPath);
        try (FileSystem fileSystem = createFileSystem()) {
            if (!fileSystem.exists(hdfsAppPath)) {
                fileSystem.mkdirs(hdfsAppPath);
                LOGGER.debug("Created HDFS directory: {}", hdfsAppPath);
            }
            int fileCount = uploadFilesRecursively(fileSystem, applicationDir, hdfsAppPath);
            LOGGER.info("Uploaded {} file(s) for application {} to {}", fileCount, applicationName, hdfsAppPath);
        } catch (IOException ex) {
            LOGGER.error("Failed to upload meta for application {}", applicationName, ex);
        }
    }

    FileSystem createFileSystem() throws IOException {
        URI uri = URI.create(FlinkScheduleConstants.HDFS_FLINK_ROOT);
        return FileSystem.get(uri, hadoopConf);
    }

    private int uploadFilesRecursively(FileSystem fileSystem, File localDir, Path hdfsDir)
            throws IOException {
        File[] children = localDir.listFiles();
        if (children == null) {
            return 0;
        }
        int uploadedCount = 0;
        for (File child : children) {
            Path targetPath = new Path(hdfsDir, child.getName());
            if (child.isDirectory()) {
                if (!fileSystem.exists(targetPath)) {
                    fileSystem.mkdirs(targetPath);
                }
                uploadedCount += uploadFilesRecursively(fileSystem, child, targetPath);
                continue;
            }
            LOGGER.debug("Copying local file {} to HDFS {}", child.getAbsolutePath(), targetPath);
            fileSystem.copyFromLocalFile(false, true, new Path(child.getAbsolutePath()), targetPath);
            uploadedCount++;
        }
        return uploadedCount;
    }
}
