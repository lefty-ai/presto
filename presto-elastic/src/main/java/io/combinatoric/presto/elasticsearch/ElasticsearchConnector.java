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

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import io.prestosql.spi.connector.*;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class ElasticsearchConnector implements Connector
{
    private static final Logger log = Logger.get(ElasticsearchConnector.class);

    private final LifeCycleManager            lifeCycleManager;
    private final ConnectorMetadata           metadata;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorSplitManager       splitManager;

    @Inject
    public ElasticsearchConnector(final LifeCycleManager lifeCycleManager,
                                  final ElasticsearchMetadata metadata,
                                  final ElasticsearchPageSourceProvider pageSourceProvider,
                                  final ElasticsearchSplitManager splitManager)
    {
        this.lifeCycleManager   = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata           = requireNonNull(metadata, "metadata is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.splitManager       = requireNonNull(splitManager, "splitManager is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return ElasticsearchTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
