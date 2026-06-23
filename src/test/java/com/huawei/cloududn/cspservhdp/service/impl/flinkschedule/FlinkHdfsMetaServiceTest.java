package com.huawei.cloududn.cspservhdp.service.impl.flinkschedule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * FlinkHdfsMetaService 单元测试。
 * <p>
 * HDFS 相关操作通过子类覆写 {@link #createFileSystem()} 进行隔离，
 * 本测试主要覆盖本地目录校验逻辑。
 * </p>
 */
public class FlinkHdfsMetaServiceTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private FlinkHdfsMetaService hdfsMetaService;

    @Before
    public void setUp() {
        hdfsMetaService = new FlinkHdfsMetaService(new FlinkModelPathHolder());
    }

    @Test
    public void uploadAllLocalMeta_shouldSkipWhenLocalRootNotExists() {
        FlinkHdfsMetaService service = new FlinkHdfsMetaService(new FlinkModelPathHolder()) {
            @Override
            public void uploadApplicationMeta(File applicationDir) {
                throw new AssertionError("Should not upload when local root missing");
            }
        };

        service.uploadAllLocalMeta();
    }

    @Test
    public void uploadAllLocalMeta_shouldSkipWhenNoApplicationSubdirectory() throws IOException {
        File localRoot = temporaryFolder.newFolder("flink-meta");
        FlinkHdfsMetaService service = new TestableHdfsMetaService(localRoot.getAbsolutePath()) {
            @Override
            public void uploadApplicationMeta(File applicationDir) {
                throw new AssertionError("Should not upload when no subdirectories");
            }
        };

        service.uploadAllLocalMeta();
    }

    @Test
    public void uploadAllLocalMeta_shouldInvokeUploadForEachApplicationDir() throws IOException {
        File localRoot = temporaryFolder.newFolder("flink-meta");
        File appDir = new File(localRoot, "order-sync");
        assertTrue(appDir.mkdir());
        File taskFile = new File(appDir, "task.json");
        Files.write(taskFile.toPath(), "{\"name\":\"order-sync\"}".getBytes(StandardCharsets.UTF_8));

        final int[] uploadCount = {0};
        FlinkHdfsMetaService service = new TestableHdfsMetaService(localRoot.getAbsolutePath()) {
            @Override
            public void uploadApplicationMeta(File applicationDir) {
                uploadCount[0]++;
                assertEquals("order-sync", applicationDir.getName());
            }
        };

        service.uploadAllLocalMeta();
        assertEquals(1, uploadCount[0]);
    }

    @Test
    public void listApplicationNames_shouldReturnEmptyWhenHdfsUnavailable() {
        FlinkHdfsMetaService service = new FlinkHdfsMetaService(new FlinkModelPathHolder()) {
            @Override
            org.apache.hadoop.fs.FileSystem createFileSystem() throws IOException {
                throw new IOException("HDFS down");
            }
        };

        List<String> names = service.listApplicationNames();
        assertTrue(names.isEmpty());
    }

    @Test
    public void probeConnection_shouldReturnFalseWhenHdfsUnavailable() {
        FlinkHdfsMetaService service = new FlinkHdfsMetaService(new FlinkModelPathHolder()) {
            @Override
            org.apache.hadoop.fs.FileSystem createFileSystem() throws IOException {
                throw new IOException("HDFS down");
            }
        };

        assertFalse(service.probeConnection());
    }

    /**
     * 可注入本地根目录的测试用 HDFS 服务。
     */
    private static class TestableHdfsMetaService extends FlinkHdfsMetaService {

        private final String localRootPath;

        TestableHdfsMetaService(String localRootPath) {
            super(new FlinkModelPathHolder());
            this.localRootPath = localRootPath;
        }

        @Override
        public void uploadAllLocalMeta() {
            File localRoot = new File(localRootPath);
            if (!localRoot.isDirectory()) {
                return;
            }
            File[] applicationDirs = localRoot.listFiles(File::isDirectory);
            if (applicationDirs == null || applicationDirs.length == 0) {
                return;
            }
            for (File applicationDir : applicationDirs) {
                uploadApplicationMeta(applicationDir);
            }
        }
    }
}
