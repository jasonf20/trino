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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.delete.DeleteManager;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorDynamicFilterProvider;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class IcebergDynamicFilterProvider
        implements ConnectorDynamicFilterProvider
{
    private static final Logger log = Logger.get(IcebergDynamicFilterProvider.class);

    private final HashMap<QueryId, DeleteManager> deleteManagers;

    @Inject
    public IcebergDynamicFilterProvider()
    {
        this.deleteManagers = new HashMap<>();
    }

    @Override
    public void queryCreated(QueryId queryId)
    {
        synchronized (deleteManagers) {
            deleteManagers.put(queryId, new DeleteManager());
        }
    }

    @Override
    public void queryCompleted(QueryId queryId)
    {
        synchronized (deleteManagers) {
            deleteManagers.remove(queryId);
        }
    }

    private DeleteManager getDeleteManager(QueryId queryId)
    {
        DeleteManager result = null;
        synchronized (deleteManagers) {
            result = deleteManagers.get(queryId);
        }
        if (result == null) {
            log.warn(MessageFormat.format("No delete manager found for query id: {0}. Falling back to new delete manager which may reload delete files", queryId));
            return new DeleteManager();
        }
        return result;
    }

    @Override
    public DynamicFilter getDynamicFilter(DynamicFilter baseFilter, QueryId queryId)
    {
        return new IcebergDynamicFilter(baseFilter, getDeleteManager(queryId));
    }

    static class IcebergDynamicFilter
            implements DynamicFilter
    {
        private final DynamicFilter delegate;
        private final DeleteManager deleteManager;

        IcebergDynamicFilter(DynamicFilter delegate,
                DeleteManager deleteManager)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.deleteManager = requireNonNull(deleteManager, "deleteManager is null");
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return delegate.getColumnsCovered();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public boolean isComplete()
        {
            return delegate.isComplete();
        }

        @Override
        public boolean isAwaitable()
        {
            return delegate.isAwaitable();
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return delegate.getCurrentPredicate();
        }

        public DeleteManager getDeleteManager()
        {
            return deleteManager;
        }
    }
}
