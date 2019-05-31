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

import static java.nio.file.StandardOpenOption.CREATE_NEW;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.snapshot.impl.DefaultSnapshotRestoreRequest;
import io.zeebe.logstreams.state.SnapshotChunk;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.util.collection.Tuple;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.zip.CRC32;
import org.slf4j.Logger;

public class RestoreSnapshotReplicator {

  private final RestoreClient client;
  private final SnapshotRestoreContext restoreContext;
  private int partitionId;
  private final Executor executor;
  private final Logger logger;
  private int numChunks;
  private final StateStorage stateStorage;

  public RestoreSnapshotReplicator(
      RestoreClient client,
      SnapshotRestoreContext restoreContext,
      int partitionId,
      Executor executor,
      Logger logger) {
    this.client = client;
    this.restoreContext = restoreContext;
    this.partitionId = partitionId;
    this.executor = executor;
    this.stateStorage = restoreContext.getStateStorage(partitionId);
    this.logger = logger;
  }

  public CompletableFuture<Tuple<Long, Long>> replicate(
      MemberId server, long snapshotId, int numChunks) {
    this.numChunks = numChunks;
    final CompletableFuture<Tuple<Long, Long>> result = new CompletableFuture<>();
    replicateInternal(server, snapshotId, 0, result);
    return result;
  }

  private void replicateInternal(
      MemberId server, long snapshotId, int chunkIdx, CompletableFuture<Tuple<Long, Long>> future) {
    final DefaultSnapshotRestoreRequest request =
        new DefaultSnapshotRestoreRequest(snapshotId, chunkIdx);
    client
        .requestSnapshotChunk(server, request)
        .whenCompleteAsync(
            (r, e) -> {
              if (e != null) {
                future.completeExceptionally(e);
              } else if (!writeChunkToDisk(r.getSnapshotChunk(), stateStorage)) {
                future.completeExceptionally(e);
              } else if (chunkIdx + 1 < numChunks) {
                replicateInternal(server, snapshotId, chunkIdx + 1, future);
              } else if (tryToMarkSnapshotAsValid(stateStorage, snapshotId)) {
                final Long exportedPosition =
                    restoreContext.getExporterPositionSupplier(stateStorage).get();
                final Long processedPosition =
                    restoreContext.getProcessorPositionSupplier(partitionId, stateStorage).get();
                future.complete(new Tuple(exportedPosition, processedPosition));
              } else {
                future.completeExceptionally(e);
              }
            },
            executor);
  }

  // TODO: following methods copied from ReplicaitonController

  private static long createChecksum(byte[] content) {
    final CRC32 crc32 = new CRC32();
    crc32.update(content);
    return crc32.getValue();
  }

  private boolean writeChunkToDisk(SnapshotChunk snapshotChunk, StateStorage storage) {
    final long snapshotPosition = snapshotChunk.getSnapshotPosition();
    final String snapshotName = Long.toString(snapshotPosition);
    final String chunkName = snapshotChunk.getChunkName();

    if (storage.existSnapshot(snapshotPosition)) {
      return true;
    }

    final long expectedChecksum = snapshotChunk.getChecksum();
    final long actualChecksum = createChecksum(snapshotChunk.getContent());

    if (expectedChecksum != actualChecksum) {
      return false;
    }

    final File tmpSnapshotDirectory = storage.getTmpSnapshotDirectoryFor(snapshotName);
    if (!tmpSnapshotDirectory.exists()) {
      tmpSnapshotDirectory.mkdirs();
    }

    final File snapshotFile = new File(tmpSnapshotDirectory, chunkName);
    if (snapshotFile.exists()) {
      logger.debug("Received a snapshot file which already exist '{}'.", snapshotFile);
      return false;
    }

    logger.debug("Consume snapshot chunk {}", chunkName);
    return writeReceivedSnapshotChunk(snapshotChunk, snapshotFile);
  }

  private boolean writeReceivedSnapshotChunk(SnapshotChunk snapshotChunk, File snapshotFile) {
    try {
      Files.write(
          snapshotFile.toPath(), snapshotChunk.getContent(), CREATE_NEW, StandardOpenOption.WRITE);
      logger.debug("Wrote replicated snapshot chunk to file {}", snapshotFile.toPath());
      return true;
    } catch (IOException ioe) {
      logger.error(
          "Unexpected error occurred on writing an snapshot chunk to '{}'.", snapshotFile, ioe);
      return false;
    }
  }

  private boolean tryToMarkSnapshotAsValid(StateStorage storage, long snapshotId) {

    final File validSnapshotDirectory = storage.getSnapshotDirectoryFor(snapshotId);

    final File tmpSnapshotDirectory = storage.getTmpSnapshotDirectoryFor(Long.toString(snapshotId));

    try {
      Files.move(tmpSnapshotDirectory.toPath(), validSnapshotDirectory.toPath());
      return true;
    } catch (FileAlreadyExistsException e) {
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }
}
