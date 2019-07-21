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
package io.combinatoric.presto.elasticsearch.scan;

import io.prestosql.spi.PrestoException;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import io.airlift.log.Logger;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import io.combinatoric.presto.elasticsearch.ElasticsearchColumnHandle;
import io.combinatoric.presto.elasticsearch.ElasticsearchSplit;
import io.combinatoric.presto.elasticsearch.ElasticsearchErrorCode;

import static java.util.Objects.requireNonNull;

/**
 * A scanner, darkly.
 */
public class Scanner
{
    private static final Logger logger = Logger.get(Scanner.class);

    private final RestHighLevelClient client;
    private final ElasticsearchSplit  split;
    private final int                 fetchSize;
    private final int                 timeout;
    private final int                 shard;

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private Future<SearchResponse> future = null;

    public Scanner(RestHighLevelClient client, ElasticsearchSplit split, List<ElasticsearchColumnHandle> columns, int shard)
    {
        this.split     = requireNonNull(split, "split is null");
        this.client    = client;
        this.fetchSize = split.getFetchSize();
        this.shard     = split.getShard();
        this.timeout   = (int) split.getTimeout().toMillis();

        logger.info("Initialized scan: [fetch-size: %d] [timeout: %d ms] [%d][%s] %s",
                fetchSize, timeout, shard, split.getTable(),
                columns.stream().map(ElasticsearchColumnHandle::getColumnName).collect(Collectors.toList()));
    }

    public void scan()
    {
        SearchRequest request = new SearchRequest(split.getTable().getTableName());

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.size(fetchSize);
        if (split.getTotalShards() > 1) {
            source.slice(new SliceBuilder(split.getShard(), split.getTotalShards()));
        }

        request.source(source);
        request.scroll(TimeValue.timeValueMillis(timeout));

        logger.info("Submitting search on: [%d][%s]", shard, split.getTable());
        future = executor.submit(new Callable<SearchResponse>()
        {
            @Override
            public SearchResponse call() throws Exception {
                return client.search(request, RequestOptions.DEFAULT);
            }
        });
    }

    public void scan(String scrollId)
    {
        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMillis(timeout));
        logger.info("Submitting scroll on: [%d][%s]", shard, split.getTable());

        future = executor.submit(new Callable<SearchResponse>()
        {
            @Override
            public SearchResponse call() throws Exception
            {
                return client.scroll(request, RequestOptions.DEFAULT);
            }
        });
    }

    public SearchResponse response()
    {
        if (future == null) {
            throw new PrestoException(ElasticsearchErrorCode.ELASTICSEARCH_SEARCH_ERROR, "Search response future is null");
        }

        try {
            return future.get();
        }
        catch (Throwable t) {
            throw new PrestoException(ElasticsearchErrorCode.ELASTICSEARCH_SEARCH_ERROR, "Failed to read search response", t);
        }
    }

    public void close(String scrollId)
    {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);

        client.clearScrollAsync(request, RequestOptions.DEFAULT, new ActionListener<ClearScrollResponse>()
        {
            @Override
            public void onResponse(ClearScrollResponse response)
            {
                logger.info("Search context cleared for scroll on: [%d][%s]", shard, split.getTable());
            }

            @Override
            public void onFailure(Exception e)
            {
                logger.error("Failed to clear search context for scroll on: [%d][%s]", shard, split.getTable(), e);
            }
        });
    }
}
