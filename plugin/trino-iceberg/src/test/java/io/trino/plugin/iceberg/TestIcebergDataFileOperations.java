/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.plugin.iceberg.util.TrackingFileSystemUtils;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResultWithQueryId;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.SystemSessionProperties.MIN_INPUT_SIZE_PER_TASK;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.plugin.iceberg.util.TrackingFileSystemUtils.FileType.DATA;
import static io.trino.plugin.iceberg.util.TrackingFileSystemUtils.FileType.DELETE;
import static io.trino.plugin.iceberg.util.TrackingFileSystemUtils.getOperations;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true) // e.g. trackingFileSystemFactory is shared mutable state
public class TestIcebergDataFileOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    private static final ImmutableSet<TrackingFileSystemUtils.FileType> DATA_FILE_TYPES = ImmutableSet.of(DATA, DELETE);

    private HiveMetastore metastore;

    private File metastoreDir;

    private TrackingFileSystemFactory trackingFileSystemFactory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                // It is essential to disable DeterminePartitionCount rule since all queries in this test scans small
                // amount of data which makes them run with single hash partition count. However, this test requires them
                // to run over multiple nodes.
                .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "0MB")
                .build();

        Path tempDir = Files.createTempDirectory("test_iceberg_data_file_ops");
        metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                // Tests that inspect MBean attributes need to run with just one node, otherwise
                // the attributes may come from the bound class instance in non-coordinator node
                .setNodeCount(1)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();

        trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));
        queryRunner.installPlugin(new TestingIcebergPlugin(
                Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)),
                Optional.of(trackingFileSystemFactory),
                binder -> {
                    newOptionalBinder(binder, Key.get(boolean.class, AsyncIcebergSplitProducer.class))
                            .setBinding().toInstance(false);
                }));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @Test
    public void testV2TableEnsureEqualityDeleteFilesAreReadOnce()
            throws Exception
    {
        String tableName = "test_equality_deletes_ensure_delete_read_count" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, age INT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 20), (3, 30)", 2);
        // change the schema and do another insert to force at least 2 splits
        // use the same ID in both files so the delete file doesn't get optimized away by statstics
        assertUpdate("INSERT INTO " + tableName + "  VALUES (2, 22)", 1);
        Table icebergTable = loadTable(tableName);

        // Delete only 1 row in the file so the data file is not pruned completely
        writeEqualityDeleteForTable(icebergTable,
                metastoreDir,
                trackingFileSystemFactory,
                Optional.of(icebergTable.spec()),
                Optional.empty(),
                ImmutableMap.of("id", 2),
                Optional.empty());

        var expectedAccesses = ImmutableMultiset.<TrackingFileSystemUtils.FileOperation>builder()
                .addCopies(new TrackingFileSystemUtils.FileOperation(DATA, INPUT_FILE_NEW_STREAM), 2)
                .addCopies(new TrackingFileSystemUtils.FileOperation(DELETE, INPUT_FILE_NEW_STREAM), 1)
                .build();

        resetCounts();
        MaterializedResultWithQueryId queryResult = getDistributedQueryRunner().executeWithQueryId(getSession(), "SELECT * FROM " + tableName);
        assertEquals(queryResult.getResult().getRowCount(), 1);
        assertMultisetsEqual(
                getOperations(trackingFileSystemFactory).stream()
                        .filter(operation -> DATA_FILE_TYPES.contains(operation.fileType()))
                        .collect(toImmutableMultiset()),
                expectedAccesses);
        assertUpdate("DROP TABLE " + tableName);
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, trackingFileSystemFactory, "iceberg", "test_schema");
    }

    private void resetCounts()
    {
        trackingFileSystemFactory.reset();
    }
}
