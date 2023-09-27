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
package io.trino.plugin.iceberg.delete;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;

import java.util.HashMap;
import java.util.List;

import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
        implements DeleteFilter
{
    /**
     * Box class for data sequence number to avoid auto-boxing when adding values to the delete map
     */
    private static class DataSequenceNumber
    {
        private long dataSequenceNumber;

        public DataSequenceNumber(long dataSequenceNumber)
        {
            this.dataSequenceNumber = dataSequenceNumber;
        }
    }

    public static final class FileToken
    {
        private long dataVersion;
        private boolean isLoaded;
        private boolean isLoading;
        private final Object lockToken;

        private FileToken(long dataVersion)
        {
            this.dataVersion = dataVersion;
            this.isLoaded = false;
            this.isLoading = false;
            this.lockToken = new Object();
        }

        public boolean isLoaded()
        {
            synchronized (lockToken) {
                return isLoaded;
            }
        }

        public boolean startLoading()
        {
            synchronized (lockToken) {
                if (isLoaded || isLoading) {
                    return false;
                }
                else {
                    isLoading = true;
                    return true;
                }
            }
        }

        public long dataVersion()
        {
            return dataVersion;
        }

        public void setDataVersion(long newDataVersion)
        {
            this.dataVersion = newDataVersion;
        }

        public void setLoaded()
        {
            synchronized (lockToken) {
                this.isLoaded = true;
                this.isLoading = false;
            }
        }

        public void resetLoading()
        {
            synchronized (lockToken) {
                isLoading = false;
            }
        }
    }

    private final Schema deleteSchema;
    private final StructLikeMap<DataSequenceNumber> deleteMap;
    private final HashMap<String, FileToken> loadedFiles;
    private final Object containerReference;

    /**
     * @param containerReference This is passed in since we don't want the weak-reference holding this objects container to be nulled out while this
     * equality delete filter is still in use.
     */
    public EqualityDeleteFilter(Schema deleteSchema, Object containerReference)
    {
        this.deleteSchema = requireNonNull(deleteSchema, "deleteSchema is null");
        this.deleteMap = StructLikeMap.create(deleteSchema.asStruct());
        this.loadedFiles = new HashMap<>();
        this.containerReference = containerReference;
    }

    public FileToken getLoadFileToken(String fileName, long dataSequenceNumber)
    {
        synchronized (loadedFiles) {
            FileToken fileToken = loadedFiles.get(fileName);
            if (fileToken == null) {
                fileToken = new FileToken(dataSequenceNumber);
                loadedFiles.put(fileName, fileToken);
                return fileToken;
            }
            else if (fileToken.isLoaded() && fileToken.dataVersion() >= dataSequenceNumber) {
                return null;
            }
            else {
                fileToken.setDataVersion(dataSequenceNumber);
                return fileToken;
            }
        }
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns, long dataSequenceNumber)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        Schema fileSchema = schemaFromHandles(columns);
        boolean hasRequiredColumns = columns.size() >= fileSchema.columns().size();
        for (Types.NestedField column : deleteSchema.columns()) {
            hasRequiredColumns = hasRequiredColumns && fileSchema.findField(column.fieldId()) != null;
        }
        if (!hasRequiredColumns) {
            // If we don't have all the required columns this delete filter can't be applied.
            // The iceberg split manager is responsible for making sure that we have all the required columns when a delete filter should be applied
            return ((page, position) -> true);
        }
        else {
            StructProjection projection = StructProjection.create(fileSchema, deleteSchema);

            return (page, position) -> {
                StructLike row = new LazyTrinoRow(types, page, position);
                DataSequenceNumber maxDeleteVersion = deleteMap.get(projection.wrap(row));
                return maxDeleteVersion == null || maxDeleteVersion.dataSequenceNumber <= dataSequenceNumber;
            };
        }
    }

    public void loadEqualityDeletes(ConnectorPageSource pageSource,
            List<IcebergColumnHandle> columns,
            long dataSequenceNumber)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        DataSequenceNumber boxedDataSequenceNumber = new DataSequenceNumber(dataSequenceNumber);
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                TrinoRow key = new TrinoRow(types, page, position);
                synchronized (deleteMap) {
                    DataSequenceNumber existingSequenceNumber = deleteMap.put(key, boxedDataSequenceNumber);
                    if (existingSequenceNumber != null && existingSequenceNumber.dataSequenceNumber > dataSequenceNumber) {
                        deleteMap.put(key, existingSequenceNumber);
                    }
                }
            }
        }
    }

    /**
     * Stub method to ensure containerReference isn't optimized away
     */
    public Object getContainerReference()
    {
        return containerReference;
    }
}
