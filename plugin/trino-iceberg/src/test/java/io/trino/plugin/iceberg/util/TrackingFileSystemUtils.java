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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.trino.filesystem.TrackingFileSystemFactory;

import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public final class TrackingFileSystemUtils
{
    private TrackingFileSystemUtils()
    {
    }

    public record FileOperation(TrackingFileSystemUtils.FileType fileType, TrackingFileSystemFactory.OperationType operationType)
    {
        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    public enum FileType
    {
        METADATA_JSON,
        SNAPSHOT,
        MANIFEST,
        STATS,
        DATA,
        DELETE,
        /**/;

        public static FileType fromFilePath(String path)
        {
            if (path.endsWith("metadata.json")) {
                return METADATA_JSON;
            }
            if (path.contains("/snap-")) {
                return SNAPSHOT;
            }
            if (path.endsWith("-m0.avro")) {
                return MANIFEST;
            }
            if (path.endsWith(".stats")) {
                return STATS;
            }
            if (path.contains("/delete_file") && (path.endsWith(".orc") || path.endsWith(".parquet"))) {
                return DELETE;
            }
            if (path.contains("/data/") && (path.endsWith(".orc") || path.endsWith(".parquet"))) {
                return DATA;
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }

    public static Multiset<FileOperation> getOperations(TrackingFileSystemFactory fileSystemFactory)
    {
        return fileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new FileOperation(
                        FileType.fromFilePath(entry.getKey().location().toString()),
                        entry.getKey().operationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }
}
