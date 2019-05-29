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
package io.zeebe.distributedlog.restore.snapshot;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.restore.RestoreClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;

public class SnapshotReplicator {

  private final RestoreClient client;
  private final Executor executor;
  private final Logger logger;
  private int numChunks;

  public SnapshotReplicator(RestoreClient client, Executor executor, Logger logger) {
    this.client = client;
    this.executor = executor;
    this.logger = logger;
  }

  public CompletableFuture<Void> replicate(MemberId server, long snapshotId, int numChunks) {
    this.numChunks = numChunks;
    final CompletableFuture<Void> result = new CompletableFuture<>();
    replicateInternal(server, snapshotId, 0, result);
    return result;
  }

  private void replicateInternal(
      MemberId server, long snapshotId, int chunkIdx, CompletableFuture<Void> future) {
    /*  SnapshotRequest request;
    client
        .requestSnapshotChunk(request)
        .whenCompleteAsync(
            (r, e) -> {
              if (e != null) {
                future.completeExceptionally(null);

              } else if (!writeChunkToDisk(r.getChunk())) {
                future.completeExceptionally(null);
              } else if (chunkIdx + 1 < numChunks) {
                replicateInternal(server, snapshotId, chunkIdx + 1, future);
              } else if (tryMarkSnapshotValid()) {
                future.complete(null);
              } else {
                future.completeExceptionally(null);
              }
            },
            executor);*/
  }
}
