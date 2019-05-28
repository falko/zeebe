/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
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
package io.zeebe.distributedlog.restore.impl;

import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreFactory;
import io.zeebe.distributedlog.restore.RestoreNodeProvider;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.log.LogReplicationAppender;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.logstreams.state.SnapshotRequester;
import java.util.function.Supplier;
import org.slf4j.Logger;

public class RestoreController {

  private final ThreadContext restoreThreadContext;
  private final Logger logger;
  private DefaultStrategyPicker strategyPicker;

  public RestoreController(
      int partitionId, LogReplicationAppender appender, RestoreFactory clientFactory, Logger log) {
    this.restoreThreadContext =
        new SingleThreadContext(String.format("log-restore-%d", partitionId));
    logger = log;
    strategyPicker = this.buildRestoreStrategyPicker(partitionId, appender, clientFactory);
  }

  public long restore(
      long latestLocalPosition, long backupPosition, Supplier<Long> commitPositionSupplier) {
    try {
      return strategyPicker
          .pick(latestLocalPosition, backupPosition)
          .thenCompose(RestoreStrategy::executeRestoreStrategy)
          .join();
    } catch (RuntimeException e) {
      return commitPositionSupplier.get();
    }
  }

  private DefaultStrategyPicker buildRestoreStrategyPicker(
      int partitionId, LogReplicationAppender appender, RestoreFactory clientFactory) {
    final RestoreClient restoreClient = clientFactory.createClient(partitionId);
    final LogReplicator logReplicator =
        new LogReplicator(appender, restoreClient, restoreThreadContext, logger);
    final SnapshotRequester snapshotReplicator =
        new SnapshotRequester(
            restoreClient, clientFactory.createSnapshotRestoreContext(), partitionId);

    final RestoreNodeProvider nodeProvider = clientFactory.createNodeProvider(partitionId);
    return new DefaultStrategyPicker(
        restoreClient,
        nodeProvider,
        logReplicator,
        snapshotReplicator,
        restoreThreadContext,
        logger);
  }
}
