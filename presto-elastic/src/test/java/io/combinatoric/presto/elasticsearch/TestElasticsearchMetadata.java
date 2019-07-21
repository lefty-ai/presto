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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.*;

import static io.combinatoric.presto.elasticsearch.ElasticsearchConfig.DEFAULT_SCHEMA;;
import static io.combinatoric.presto.elasticsearch.TableUtil.TestTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TestElasticsearchMetadata extends IntegrationTestBase
{
    private static final Logger logger = Logger.get(TestElasticsearchMetadata.class);

    private static TestTable TEST_TABLE;

    @BeforeClass
    public static void before() throws Exception
    {
        TEST_TABLE = create(TestTable.ofAll(), 1, 1);
    }

    @AfterClass
    public static void after() throws Exception
    {
        drop(TEST_TABLE);
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
        ConnectorTableHandle handle = new ElasticsearchTableHandle(
                TEST_TABLE.schema().getSchemaName(), TEST_TABLE.schema().getTableName());

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

    @Test
    public void testGetColumnMetadata()
    {
        ConnectorTableHandle handle = new ElasticsearchTableHandle(
                TEST_TABLE.schema().getSchemaName(), TEST_TABLE.schema().getTableName());
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(SESSION, handle);

        for (Map.Entry<String, ColumnHandle> entry : handles.entrySet()) {
            ColumnMetadata cm = metadata.getColumnMetadata(SESSION, handle, entry.getValue());
            assertThat(cm.getName(), equalTo(((ElasticsearchColumnHandle) entry.getValue()).getColumnName()));
            assertThat(cm.getType(), equalTo(((ElasticsearchColumnHandle) entry.getValue()).getColumnType()));
        }
    }
}





