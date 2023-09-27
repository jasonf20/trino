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

import io.trino.plugin.iceberg.delete.DeleteManager;

import java.lang.ref.WeakReference;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Optional;

/**
 * Manages instances of {@link DeleteManager}.
 * Delete Managers should live for the duration of the query, but once the entire query is complete they
 * can be opportunistically cleaned up to free up memory.
 * If a delete manager is still available when a new query for the same table/snapshot is started it can be re-used. Avoiding reloading delete files that
 * were already loaded by the previous query.
 */
public class DeleteManagerFactory
{
    private static final int DEFAULT_DELETE_MANAGER_THRESHOLD = 16;

    private final HashMap<String, WeakReference<DeleteManager>> deleteManagers = new HashMap<>();
    private int deleteManagerCleanupThreshold = DEFAULT_DELETE_MANAGER_THRESHOLD;

    /**
     * Keep a delete manager per table partition, this makes sure we only apply the deletes to the correct tables/partitions.
     */
    public DeleteManager getDeleteManager(String catalog,
            String schemaName,
            String tableName,
            Optional<Long> snapshotId,
            String partitionData)
    {
        String key = MessageFormat.format("''{0}''.''{1}''.''{2}''-{3}-{4}", escapeDeleteManagerKey(catalog),
                escapeDeleteManagerKey(schemaName),
                escapeDeleteManagerKey(tableName),
                snapshotId, partitionData);
        return getDeleteManager(key);
    }

    private DeleteManager getDeleteManager(String tableKey)
    {
        synchronized (deleteManagers) {
            int deleteManagerCount = deleteManagers.size();
            if (deleteManagerCount >= deleteManagerCleanupThreshold) {
                cleanupDeleteManagers();
            }
            WeakReference<DeleteManager> deleteManager = deleteManagers.get(tableKey);
            if (deleteManager == null) {
                var result = new DeleteManager();
                deleteManagers.put(tableKey, new WeakReference<>(result));
                return result;
            }
            else {
                var result = deleteManager.get();
                if (result == null) {
                    result = new DeleteManager();
                    deleteManagers.put(tableKey, new WeakReference<>(result));
                }
                return result;
            }
        }
    }

    private String escapeDeleteManagerKey(String key)
    {
        return key.replace("'", "''");
    }

    private void cleanupDeleteManagers()
    {
        deleteManagers.entrySet().removeIf(dm -> dm.getValue().get() == null);
        int activeDeleteManagers = deleteManagers.size();
        if (activeDeleteManagers < deleteManagerCleanupThreshold / 4) {
            deleteManagerCleanupThreshold = Math.max(deleteManagerCleanupThreshold / 2, DEFAULT_DELETE_MANAGER_THRESHOLD);
        }
        else {
            deleteManagerCleanupThreshold = Math.max(2 * activeDeleteManagers, deleteManagerCleanupThreshold);
        }
    }
}
