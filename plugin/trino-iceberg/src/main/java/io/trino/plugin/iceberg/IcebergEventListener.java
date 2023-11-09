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
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorDynamicFilterProvider;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;

import static java.util.Objects.requireNonNull;

public class IcebergEventListener
        implements EventListener
{
    private final IcebergDynamicFilterProvider dynamicFilterProvider;

    @Inject
    public IcebergEventListener(ConnectorDynamicFilterProvider dynamicFilterProvider)
    {
        this.dynamicFilterProvider = (IcebergDynamicFilterProvider) requireNonNull(dynamicFilterProvider, "dynamicFilterProvider is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        dynamicFilterProvider.queryCreated(QueryId.valueOf(queryCreatedEvent.getMetadata().getQueryId()));
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        dynamicFilterProvider.queryCompleted(QueryId.valueOf(queryCompletedEvent.getMetadata().getQueryId()));
    }
}
