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
import io.prestosql.testing.TestingConnectorSession;

import io.airlift.log.Logger;

import com.google.common.collect.ImmutableList;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.combinatoric.presto.elasticsearch.ElasticsearchConfig.DEFAULT_SCHEMA;
import static io.combinatoric.presto.elasticsearch.TableUtil.TEST_TABLE;
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
        createTable(TEST_TABLE);
    }

    @After
    public void after() throws IOException
    {
        //dropTable(TEST_TABLE);
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
            assertThat(table.getSchemaName(), equalTo(TEST_TABLE.schema().getSchemaName()));
            assertThat(table.getTableName(), equalTo(TEST_TABLE.schema().getTableName()));
        }

        logger.info("Tables: " + tables.toString());
    }

    @Test
    public void testGetTableHandle()
    {
        ElasticsearchTableHandle handle =
                (ElasticsearchTableHandle) metadata.getTableHandle(SESSION, TEST_TABLE.schema());

        assertThat(handle.getSchemaName(), equalTo(TEST_TABLE.schema().getSchemaName()));
        assertThat(handle.getTableName(), equalTo(TEST_TABLE.schema().getTableName()));

        logger.info("Table: " + handle.toString());
    }

    @Test
    public void testGetTableMetadata()
    {
        ElasticsearchTableHandle handle = new ElasticsearchTableHandle(
                TEST_TABLE.schema().getSchemaName(), TEST_TABLE.schema().getTableName());

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, handle);
        logger.info("Table metadata: " + tableMetadata.toString());

        List<ColumnMetadata> actual = tableMetadata.getColumns().stream().sorted
                (Comparator.comparing(ColumnMetadata::getName)).collect(Collectors.toList());

        assertThat(actual.size(), equalTo(TEST_TABLE.columns().size()));

        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i).getName(), equalTo(TEST_TABLE.columns().get(i).getColumnName()));
            assertThat(actual.get(i).getType(), equalTo(TEST_TABLE.columns().get(i).getColumnType()));
        }
    }

    @Test
    public void testGetColumnHandles()
    {
        ConnectorTableHandle handle = new ElasticsearchTableHandle(TEST_TABLE.schema().getSchemaName(), TEST_TABLE.schema().getTableName());

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(SESSION, handle);
        assertThat(handles.size(), equalTo(TEST_TABLE.columns().size()));

        for (ElasticsearchColumnHandle column : TEST_TABLE.columns()) {
            assertThat(handles.containsKey(column.getColumnName()), is(true));
            ElasticsearchColumnHandle ch = (ElasticsearchColumnHandle) handles.get(column.getColumnName());
            assertThat(ch.getColumnType(), equalTo(column.getColumnType()));
        }
    }

    @Test
    public void testListTableColumns()
    {
        SchemaTablePrefix prefix = new SchemaTablePrefix(
                TEST_TABLE.schema().getSchemaName(), TEST_TABLE.schema().getTableName());

        Map<SchemaTableName, List<ColumnMetadata>> map = metadata.listTableColumns(SESSION, prefix);
        assertThat(map.size(), equalTo(1));

        for (Map.Entry<SchemaTableName, List<ColumnMetadata>> entry : map.entrySet()) {
            logger.info("key: " + entry.getKey() + " value: " + entry.getValue());
            assertThat(entry.getKey(), equalTo(TEST_TABLE.schema()));
            List<ColumnMetadata> columns =
                    entry.getValue().stream().sorted(Comparator.comparing(ColumnMetadata::getName)).collect(Collectors.toList());

            for (int i = 0; i < columns.size(); i++) {
                assertThat(columns.get(i).getName(), equalTo(TEST_TABLE.columns().get(i).getColumnName()));
                assertThat(columns.get(i).getType(), equalTo(TEST_TABLE.columns().get(i).getColumnType()));
            }
        }
    }
}





