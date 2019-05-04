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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.*;

import io.airlift.log.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;

import static java.util.Objects.requireNonNull;
import static io.combinatoric.presto.elasticsearch.ElasticsearchErrorCode.*;
import static io.combinatoric.presto.elasticsearch.type.Types.toPrestoType;
import static org.elasticsearch.action.support.IndicesOptions.Option.*;
import static org.elasticsearch.action.support.IndicesOptions.WildcardStates.OPEN;

/**
 * ElasticsearchMetadata
 * <p>
 * This class handles the translation between the Elasticsearch concepts of index/mapping to
 * the relational concepts of table/columns.
 */
public class ElasticsearchMetadata implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(ElasticsearchMetadata.class);

    private static final IndicesOptions indicesOptions = new IndicesOptions(
            EnumSet.of(IGNORE_ALIASES, FORBID_CLOSED_INDICES, ALLOW_NO_INDICES),
            EnumSet.of(OPEN));

    private final ElasticsearchClient client;
    private final ElasticsearchConfig config;

    @Inject
    public ElasticsearchMetadata(ElasticsearchClient client, ElasticsearchConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(config.getDefaultSchema());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent()) {
            if (!schemaName.get().equals(config.getDefaultSchema())) {
                throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "No such schema: " + schemaName.get());
            }
        }

        GetIndexRequest request = new GetIndexRequest()
                .indices("*")
                .indicesOptions(indicesOptions);

        try {
            GetIndexResponse response = client.client().indices().get(request, RequestOptions.DEFAULT);
            return Arrays.stream(response.indices())
                    .filter(index -> !index.startsWith("."))
                    .map(index -> new SchemaTableName(config.getDefaultSchema(), index)).collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Failed to get list of elasticsearch indices", e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        requireNonNull(table, "table is null");

        try {
            GetIndexRequest request = new GetIndexRequest().indices(table.getTableName()).indicesOptions(indicesOptions);
            GetIndexResponse response = client.client().indices().get(request, RequestOptions.DEFAULT);

            String[] indices = response.indices();
            if (indices == null || indices.length == 0) {
                return null;
            }
            if (indices.length > 1) {
                throw new PrestoException(ELASTICSEARCH_METADATA_ERROR,
                        "Multiple elasticsearch indices " + Arrays.toString(indices) + " found for table: " + table.getTableName());
            }

            return new ElasticsearchTableHandle(table.getSchemaName(), indices[0]);
        }
        catch (IOException e) {
            throw new TableNotFoundException(table, e);
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        GetMappingsResponse response;
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        try {
            GetMappingsRequest request = new GetMappingsRequest().indices(handle.getTableName());
            response = client.client().indices().getMapping(request, RequestOptions.DEFAULT);
        }
        catch (IOException e) {
            throw new TableNotFoundException(handle.getSchemaTableName(), "Unable to read table metadata", e);
        }

        if (!response.mappings().containsKey(handle.getTableName())) {
            throw new TableNotFoundException(
                    handle.getSchemaTableName(), "Metadata does not exist for table: " + handle.getTableName());
        }

        Map<String, Object> mapping = response.mappings().get(handle.getTableName()).sourceAsMap();
        List<ColumnMetadata> columns = mappings(mapping);

        return new ConnectorTableMetadata(handle.getSchemaTableName(), columns);
    }

    /**
     * Converts Elasticsearch index mappings to Presto column metadata.
     */
    @SuppressWarnings("unchecked")
    private List<ColumnMetadata> mappings(Map<String, Object> mapping)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        for (Map.Entry<String, Object> entry : ((Map<String, Object>) mapping.get("properties")).entrySet()) {

            Map<String, Object> properties = (Map<String, Object>) entry.getValue();
            String type = (String) properties.get("type");

            if (type == null || type.isEmpty() || type.equals("object") || type.equals("nested")) {
                throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: " + type);
            }
            else {
                builder.add(column(entry.getKey(), type, false));
            }
        }

        return builder.build();
    }

    private ColumnMetadata column(String name, String type, boolean hidden)
    {
        return new ColumnMetadata(name, toPrestoType(type), true, null, null, hidden, ImmutableMap.of());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return null;
    }
}
