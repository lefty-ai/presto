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
package io.combinatoric.presto.elasticsearch;

import io.prestosql.spi.connector.*;

import com.google.common.collect.ImmutableList;

import java.util.List;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSourceProvider implements ConnectorPageSourceProvider
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchPageSourceProvider(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                                                ConnectorSession session,
                                                ConnectorSplit split,
                                                ConnectorTableHandle table, List<ColumnHandle> columns)
    {
        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();

        for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
            handles.add((ElasticsearchColumnHandle) handle);
        }

        return new ElasticsearchPageSource(client, (ElasticsearchSplit) split, handles.build());
    }
}
