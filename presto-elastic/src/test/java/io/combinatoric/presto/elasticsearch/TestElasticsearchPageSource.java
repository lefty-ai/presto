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

import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;

import io.airlift.log.Logger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static io.combinatoric.presto.elasticsearch.TableUtil.TestTable;
import static io.prestosql.spi.type.DateType.DATE;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TestElasticsearchPageSource extends IntegrationTestBase
{
    private static final Logger logger = Logger.get(TestElasticsearchPageSource.class);

    @Test
    public void testPrimitiveTypes() throws Exception
    {
        TestTable table = create(TestTable.ofAll(), 3, 3);

        try {
            ElasticsearchPageSource source = new ElasticsearchPageSource(
                    client,
                    split(table),
                    table.columns());

            for (Page page = source.getNextPage(); page != null; page = source.getNextPage()) {
                verify(page, table);
            }

            source.close();
        }
        finally {
            drop(table);
        }
    }

    public void test()
    {
        String original = "2016-10-28T18:52:00.000Z";
        LocalDateTime parsed = LocalDateTime.parse(original, DateTimeFormatter.ISO_DATE_TIME);

        System.out.println("ORIGINAL: " + original);
        System.out.println("PARSED: " + parsed.toString());


        original = "2016-10-28T18:52:15.000Z";
        parsed = LocalDateTime.parse(original, DateTimeFormatter.ISO_DATE_TIME);

        System.out.println("ORIGINAL: " + original);
        System.out.println("PARSED: " + parsed.toString());

        original = "2016-10-28T18:52:15.006Z";
        parsed = LocalDateTime.parse(original, DateTimeFormatter.ISO_DATE_TIME);

        System.out.println("ORIGINAL: " + original);
        System.out.println("PARSED: " + parsed.toString());
    }

    private void verify(Page page, TestTable table)
    {
        List<Type> types = table.columns().stream()
                .map(ElasticsearchColumnHandle::getColumnType).collect(Collectors.toList());

        MaterializedResult result = MaterializedResult.resultBuilder(SESSION, types).page(page).build();

        assertThat(result.getRowCount(), equalTo(table.rows()));

        for (int currentRow = 0; currentRow < result.getRowCount(); currentRow++)
        {
            MaterializedRow row = result.getMaterializedRows().get(currentRow);

            for (int currentField = 0; currentField < row.getFieldCount(); currentField++)
            {
                Object expected = table.data().get(types.get(currentField)).get(currentRow);
                Object actual   = row.getField(currentField);

                logger.info(String.format("[%s] expected value: %s | actual value: %s",
                        table.columns().get(currentField), expected, actual));

                if (types.get(currentField).equals(DATE)) {
                    assertThat(actual.toString(), equalTo(expected));
                }
                else {
                    assertThat(actual, equalTo(expected));
                }
            }
        }
    }

    private static ElasticsearchSplit split(TestTable table)
    {
        return new ElasticsearchSplit(
                table.schema(),
                0,  // hardcoded to shard 0 for testing
                table.shards(),
                client.hosts().stream().map(h -> h.getHostName() + ":" + h.getPort()).collect(Collectors.toList()),
                ElasticsearchSessionProperties.getFetchSize(SESSION),
                ElasticsearchSessionProperties.getScrollTimeout(SESSION));
    }
}
