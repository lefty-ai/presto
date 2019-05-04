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

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.TestingConnectorSession;

import io.airlift.log.Logger;

import com.google.common.collect.ImmutableList;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.combinatoric.presto.elasticsearch.ElasticsearchConfig.DEFAULT_SCHEMA;
import static io.combinatoric.presto.elasticsearch.TableUtil.TEST_TABLE_PRIMITIVE_TYPES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TestElasticsearchMetadata extends IntegrationTestBase
{
    private static final Logger logger = Logger.get(TestElasticsearchMetadata.class);

    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());


    @Before
    public void before() throws IOException
    {
        createTable(TEST_TABLE_PRIMITIVE_TYPES);
    }

    @After
    public void after() throws IOException
    {
        dropTable(TEST_TABLE_PRIMITIVE_TYPES);
    }

    @Test
    public void testListSchemaNames()
    {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertThat(schemas, hasSize(1));
        assertThat(schemas, hasItem(DEFAULT_SCHEMA));
    }

    @Test
    public void testListTables()
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertThat(tables.size(), equalTo(1));

        for (SchemaTableName table : tables) {
            assertThat(table.getSchemaName(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.schema().getSchemaName()));
            assertThat(table.getTableName(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.schema().getTableName()));
        }

        logger.info("Tables: " + tables.toString());
    }

    @Test
    public void testGetTableHandle()
    {
        ElasticsearchTableHandle handle =
                (ElasticsearchTableHandle) metadata.getTableHandle(SESSION, TEST_TABLE_PRIMITIVE_TYPES.schema());

        assertThat(handle.getSchemaName(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.schema().getSchemaName()));
        assertThat(handle.getTableName(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.schema().getTableName()));

        logger.info("Table: " + handle.toString());
    }

    @Test
    public void testGetTableMetadata()
    {
        ElasticsearchTableHandle handle = new ElasticsearchTableHandle(
                TEST_TABLE_PRIMITIVE_TYPES.schema().getSchemaName(),
                TEST_TABLE_PRIMITIVE_TYPES.schema().getTableName());

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, handle);
        logger.info("Table metadata: " + tableMetadata.toString());

        List<ColumnMetadata> actual = tableMetadata.getColumns().stream().sorted
                (Comparator.comparing(ColumnMetadata::getName)).collect(Collectors.toList());

        assertThat(actual.size(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.columns().size()));

        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i).getName(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.columns().get(i).getColumnName()));
            assertThat(actual.get(i).getType(), equalTo(TEST_TABLE_PRIMITIVE_TYPES.columns().get(i).getColumnType()));
        }
    }
}





