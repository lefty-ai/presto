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

import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;

import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.DateType.DATE;
import static io.combinatoric.presto.elasticsearch.ElasticsearchConfig.DEFAULT_SCHEMA;

public final class TableUtil
{
    private static final Type[] primitives = { TINYINT, SMALLINT, INTEGER, BIGINT, VARCHAR, BOOLEAN, REAL, DOUBLE };
    private static final Type[] dates = { DATE };

    static class TestTable
    {
        private final SchemaTableName table;

        private int                             shards;
        private List<ElasticsearchColumnHandle> columns;
        private Map<Type, List<Object>>         data;

        TestTable(SchemaTableName table, List<ElasticsearchColumnHandle> columns)
        {
            this.table   = table;
            this.shards  = -1;
            this.data    = new HashMap<>();
            this.columns = columns.stream().sorted(
                    Comparator.comparing(ElasticsearchColumnHandle::getColumnName)).collect(Collectors.toList());

            columns.stream().forEach(c -> data.put(c.getColumnType(), new ArrayList<>()));
        }

        SchemaTableName schema()
        {
            return table;
        }

        List<ElasticsearchColumnHandle> columns()
        {
            return columns;
        }

        Map<? extends Type, List<Object>> data()
        {
            return data;
        }

        int shards()
        {
            return shards;
        }

        void shards(int s)
        {
            this.shards = s;
        }

        int rows()
        {
            if (!data.isEmpty()) {
                for (Map.Entry<Type, List<Object>> entry : data.entrySet()) {
                    return entry.getValue().size();
                }
            }

            return 0;
        }

        static TestTable ofPrimitives()
        {
            return new TestTable(new SchemaTableName(DEFAULT_SCHEMA, "primitives-table"), primitives());
        }

        static TestTable ofAll()
        {
            return new TestTable(new SchemaTableName(DEFAULT_SCHEMA, "all-types-table"), all());
        }
    }

    /**
     * Creates a list of columns of the primitive data types.
     */
    private static List<ElasticsearchColumnHandle> primitives()
    {
        ImmutableList.Builder<ElasticsearchColumnHandle> builder = ImmutableList.builder();
        for (Type t : primitives) {
            builder.add(column("my_" + t.getTypeSignature().getBase(), t));
        }
        return builder.build();
    }

    /**
     * Creates a list of columns of the date/time data types.
     */
    private static List<ElasticsearchColumnHandle> dates()
    {
        ImmutableList.Builder<ElasticsearchColumnHandle> builder = ImmutableList.builder();
        for (Type t : dates) {
            builder.add(column("my_" + t.getTypeSignature().getBase(), t));
        }

        return builder.build();
    }

    private static List<ElasticsearchColumnHandle> all()
    {
        List<ElasticsearchColumnHandle> columns = new ArrayList<>();
        columns.addAll(primitives());
        columns.addAll(dates());
        return columns;
    }

    /**
     * Creates a column of the given name and type.
     */
    private static ElasticsearchColumnHandle column(String name, Type type)
    {
        return new ElasticsearchColumnHandle(name, type);
    }

}
