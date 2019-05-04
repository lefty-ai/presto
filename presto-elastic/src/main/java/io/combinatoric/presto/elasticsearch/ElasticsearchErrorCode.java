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

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;

public enum ElasticsearchErrorCode implements ErrorCodeSupplier
{
    ELASTICSEARCH_CONNECTION_ERROR(0, EXTERNAL),        // Connection error
    ELASTICSEARCH_METADATA_ERROR(1, EXTERNAL),          // Error locating or reading metadata
    ELASTICSEARCH_UNSUPPORTED_TYPE(2, EXTERNAL),        // Unsupported data type
    ELASTICSEARCH_UNSUPPORTED_DATE_FORMAT(3, EXTERNAL), // Unsupported date format
    ELASTICSEARCH_SHARD_ERROR(4, EXTERNAL),             // Error locating or reading shard
    ELASTICSEARCH_SEARCH_ERROR(5, EXTERNAL),            // Error while searching
    ELASTICSEARCH_MALFORMED_DATA(6, EXTERNAL),          // Malformed data
    ELASTICSEARCH_DATA_ENCODING(7, EXTERNAL);           // Data encoding error

    private final ErrorCode errorCode;

    ElasticsearchErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0900_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
