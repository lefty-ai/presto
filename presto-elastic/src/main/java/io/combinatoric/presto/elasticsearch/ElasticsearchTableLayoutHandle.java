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

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;


public class ElasticsearchTableLayoutHandle implements ConnectorTableLayoutHandle
{
    private final ElasticsearchTableHandle table;
    private final TupleDomain<ColumnHandle> domain;

    @JsonCreator
    public ElasticsearchTableLayoutHandle(@JsonProperty("table") ElasticsearchTableHandle table,
                                          @JsonProperty("constraint") TupleDomain<ColumnHandle> domain)
    {
        this.table  = requireNonNull(table, "table is null");
        this.domain = requireNonNull(domain, "domain is null");
    }

    @JsonProperty
    public ElasticsearchTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return domain;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchTableLayoutHandle that = (ElasticsearchTableLayoutHandle) o;

        return Objects.equals(table, that.table) && Objects.equals(domain, that.domain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, domain);
    }
}
