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

import io.airlift.log.Logger;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.combinatoric.presto.elasticsearch.shard.ShardInfo;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch split manager.
 */
public class ElasticsearchSplitManager implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(ElasticsearchSplitManager.class);

    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle connectorTableHandle,
                                          SplitSchedulingStrategy splitSchedulingStrategy)
    {
        return new FixedSplitSource(splits((ElasticsearchTableHandle) connectorTableHandle, session));
    }

    private List<ConnectorSplit> splits(ElasticsearchTableHandle handle, ConnectorSession session)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        ShardInfo si = client.shardInfo(handle.getTableName());
        logger.info("Shard Info: " + si);

        for (Map.Entry<Integer, ShardInfo.ShardStore> entry : si.stores().entrySet()) {
            logger.info("SHARD ID: " + entry.getKey());
            logger.info("SHARD STORE: " + entry.getValue());
        }


        for (int i = 0; i < handle.getNumberOfShards(); i++)
        {
            splits.add(new ElasticsearchSplit(
                    handle.getSchemaTableName(),
                    i,
                    handle.getNumberOfShards(),
                    client.hosts().stream().map(h -> h.getHostName() + ":" + h.getPort()).collect(Collectors.toList()),
                    ElasticsearchSessionProperties.getFetchSize(session),
                    ElasticsearchSessionProperties.getScrollTimeout(session)));
        }

        return splits.build();
    }
}
