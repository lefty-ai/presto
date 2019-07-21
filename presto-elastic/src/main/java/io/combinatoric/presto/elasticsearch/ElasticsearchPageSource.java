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
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import io.combinatoric.presto.elasticsearch.scan.Scanner;

import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.DateType.DATE;

import static java.util.Objects.requireNonNull;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static io.combinatoric.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_UNSUPPORTED_TYPE;

public class ElasticsearchPageSource implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ElasticsearchPageSource.class);

    private final Scanner            scanner;
    private final List<Type>         types;
    private final PageBuilder        pb;
    private final ElasticsearchSplit split;

    private boolean finished = false;
    private long totalReceivedHits = 0;

    private final List<ElasticsearchColumnHandle> columns;

    public ElasticsearchPageSource(ElasticsearchClient client,
                                   ElasticsearchSplit split,
                                   List<ElasticsearchColumnHandle> columns)
    {
        checkArgument(columns != null && columns.size() > 0, "columns is null or empty");

        this.split   = requireNonNull(split, "split is null");
        this.columns = columns;
        this.types   = columns.stream().map(ElasticsearchColumnHandle::getColumnType).collect(Collectors.toList());
        this.scanner = new Scanner(client.client(), split, columns, split.getShard());
        this.pb      = new PageBuilder(types);

        scanner.scan(); // initiate scan asynchronously
    }

    @Override
    public Page getNextPage()
    {
        SearchResponse response = scanner.response();

        if (response.getHits().getHits().length == 0) {
            finished = true;
            scanner.close(response.getScrollId());
            return null;
        }

        totalReceivedHits += response.getHits().getHits().length;
        logger.info("Received %d out of running total %d hits for scan on [%d][%s]",
                response.getHits().getHits().length, totalReceivedHits, split.getShard(), split.getTable());

        scanner.scan(response.getScrollId());
        return page(response);
    }

    private Page page(SearchResponse response)
    {
        checkState(pb.isEmpty());

        for (SearchHit hit : response.getHits().getHits())
        {
            pb.declarePosition();
            Map<String, Object> source = hit.getSourceAsMap();

            for (int i = 0; i < columns.size(); i++)
            {
                BlockBuilder block = pb.getBlockBuilder(i);
                append(columns.get(i), source.get(columns.get(i).getColumnName()), block);
            }

        }

        Page page = pb.build();
        pb.reset();
        return page;
    }

    private void append(ElasticsearchColumnHandle column, Object value, BlockBuilder block)
    {
        if (value == null) {
            block.appendNull();
            return;
        }

        Type type = column.getColumnType();
        Class<?> java = type.getJavaType();

        //logger.info("COLUMN: %s %s\t%s", column.getColumnName(), column.getColumnType(), value.toString());

        if (java == long.class) {
            writeLongType(type, value, block);
        }
        else if (java == double.class) {
            type.writeDouble(block, Double.parseDouble(value.toString()));
        }
        else if (java == Slice.class) {
            writeSliceType(type, value, block);
        }
        else if (java == boolean.class) {
            type.writeBoolean(block, Boolean.parseBoolean(value.toString()));
        }
        else {
            throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }

    private void writeLongType(Type type, Object value, BlockBuilder block)
    {
        if (type.equals(BIGINT)) {
            type.writeLong(block, Long.parseLong(value.toString()));
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(block, Integer.parseInt(value.toString()));
        }
        else if (type.equals(SMALLINT)) {
            type.writeLong(block, Short.parseShort(value.toString()));
        }
        else if (type.equals(TINYINT)) {
            type.writeLong(block, Byte.parseByte(value.toString()));
        }
        else if (type.equals(REAL)) {
            type.writeLong(block, floatToRawIntBits(Float.parseFloat(value.toString())));
        }
        else if (type.equals(DATE)) {
            LocalDate date = LocalDate.parse((String) value);
            type.writeLong(block, date.toEpochDay());
        }
        else {
            throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }

    private void writeSliceType(Type type, Object value, BlockBuilder block)
    {
        String base = type.getTypeSignature().getBase();
        if (base.equals(StandardTypes.VARCHAR)) {
            type.writeSlice(block, utf8Slice(value.toString()));
        }
        else {
            throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        // XXX Implement with closing scroll context
    }
}
