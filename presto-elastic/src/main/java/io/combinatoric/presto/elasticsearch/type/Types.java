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
package io.combinatoric.presto.elasticsearch.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.combinatoric.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_UNSUPPORTED_TYPE;


public class Types
{
    /**
     * Given the name of an Elasticsearch type, returns the Presto type.
     */
    public static Type toPrestoType(String type)
    {
        switch (type) {
            case "integer":
                return INTEGER;
            case "long":
                return BIGINT;
            case "short":
                return SMALLINT;
            case "byte":
                return TINYINT;
            case "boolean":
                return BOOLEAN;
            case "keyword":
            case "text":
                return VARCHAR;
            case "float":
                return REAL;
            case "double":
                return DOUBLE;
            case "date":
                return DATE;
            default:
                throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }

    /**
     * Given a Presto type, returns the name of the Elasticsearch type.
     */
    public static String toElasticsearchTypeName(final Type type)
    {
        switch (type.getTypeSignature().getBase()) {
            case "integer":
                return "integer";
            case "bigint":
                return "long";
            case "tinyint":
                return "byte";
            case "smallint":
                return "short";
            case "boolean":
                return "boolean";
            case "varchar":
                return "keyword";
            case "real":
                return "float";
            case "double":
                return "double";
            case "date":
                return "date";
            default:
                throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }
}
