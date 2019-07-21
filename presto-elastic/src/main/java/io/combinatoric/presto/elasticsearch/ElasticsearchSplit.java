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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.SchemaTableName;

import io.airlift.units.Duration;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


public class ElasticsearchSplit implements ConnectorSplit
{
    private final int          shard;
    private final int          totalShards;
    private final int          fetchSize;
    private final Duration     timeout;
    private final List<String> nodes;
    private final SchemaTableName table;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("table") SchemaTableName table,
            @JsonProperty("shard") int shard,
            @JsonProperty("totalShards") int totalShards,
            @JsonProperty("nodes") List<String> nodes,
            @JsonProperty("fetchSize") int fetchSize,
            @JsonProperty("scrollTimeout") Duration timeout)
    {
        this.table       = requireNonNull(table, "table is null");
        this.nodes       = requireNonNull(nodes, "node is null");
        this.shard       = shard;
        this.totalShards = totalShards;
        this.fetchSize   = fetchSize;
        this.timeout     = timeout;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return nodes.stream().map(
                n -> HostAddress.fromString(n).withDefaultPort(9200)).collect(Collectors.toList());
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }

    @JsonProperty
    public SchemaTableName getTable()
    {
        return table;
    }

    @JsonProperty
    public int getTotalShards()
    {
        return totalShards;
    }

    @JsonProperty
    public List<String> getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public int getFetchSize()
    {
        return fetchSize;
    }

    @JsonProperty
    public Duration getTimeout()
    {
        return timeout;
    }
}
