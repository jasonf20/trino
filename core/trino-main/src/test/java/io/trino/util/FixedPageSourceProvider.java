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
package io.trino.util;

import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.split.PageSourceProvider;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class FixedPageSourceProvider
        implements PageSourceProvider
{
    public interface PageSourceBuilder
    {
        ConnectorPageSource buildPageSource(
                Session session,
                Split split,
                TableHandle table,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter);
    }

    private final PageSourceBuilder pageSourceBuilder;

    public FixedPageSourceProvider(PageSourceBuilder pageSourceBuilder)
    {
        this.pageSourceBuilder = requireNonNull(pageSourceBuilder, "pageSourceBuilder is null");
    }

    @Override
    public PageSourceProvider getStatefulInstance(CatalogHandle catalogHandle)
    {
        return this;
    }

    @Override
    public ConnectorPageSource createPageSource(Session session, Split split, TableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        return this.pageSourceBuilder.buildPageSource(session, split, table, columns, dynamicFilter);
    }
}
