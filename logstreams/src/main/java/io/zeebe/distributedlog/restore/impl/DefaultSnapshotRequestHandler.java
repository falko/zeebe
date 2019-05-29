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

import io.zeebe.distributedlog.restore.RestoreServer.SnapshotRequestHandler;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreRequest;
import io.zeebe.distributedlog.restore.snapshot.impl.DefaultSnapshotRestoreResponse;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.state.SnapshotChunk;
import io.zeebe.logstreams.state.StateStorage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.slf4j.Logger;

public class DefaultSnapshotRequestHandler implements SnapshotRequestHandler {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;
  private final StateStorage snapshotStorage;

  public DefaultSnapshotRequestHandler(StateStorage snapshotStorage) {
    this.snapshotStorage = snapshotStorage;
  }

  @Override
  public DefaultSnapshotRestoreResponse onSnapshotRequest(SnapshotRestoreRequest request) {
    LOG.trace("Replicating snapshot on demand");
    final File snapshotDirectory = snapshotStorage.getSnapshotDirectoryFor(request.getSnapshotId());
    if (snapshotDirectory.exists()) {
      final File[] files = snapshotDirectory.listFiles();
      Arrays.sort(files);
      if (request.getChunkIdx() < files.length) {
        final SnapshotChunk snapshotChunk =
            createSnapshotChunkFromFile(
                files[request.getChunkIdx()], request.getSnapshotId(), files.length);
        if (snapshotChunk != null) {
          return new DefaultSnapshotRestoreResponse(true, snapshotChunk);
        }
      }
    }
    // TODO: Handle error cases
    return new DefaultSnapshotRestoreResponse(false, null);
  }

  // TODO: following methods copied from ReplicaitonController

  private static long createChecksum(byte[] content) {
    final CRC32 crc32 = new CRC32();
    crc32.update(content);
    return crc32.getValue();
  }

  private SnapshotChunk createSnapshotChunkFromFile(
      File snapshotChunkFile, long snapshotPosition, int totalCount) {
    final byte[] content;
    try {
      content = Files.readAllBytes(snapshotChunkFile.toPath());

      final long checksum = createChecksum(content);
      return new SnapshotChunkImpl(
          snapshotPosition, totalCount, snapshotChunkFile.getName(), checksum, content);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  // TODO: Following is copied from ReplicationController. Fix duplicates
  private final class SnapshotChunkImpl implements SnapshotChunk {
    private final long snapshotPosition;
    private final int totalCount;
    private final String chunkName;
    private final byte[] content;
    private final long checksum;

    SnapshotChunkImpl(
        long snapshotPosition, int totalCount, String chunkName, long checksum, byte[] content) {
      this.snapshotPosition = snapshotPosition;
      this.totalCount = totalCount;
      this.chunkName = chunkName;
      this.checksum = checksum;
      this.content = content;
    }

    public long getSnapshotPosition() {
      return snapshotPosition;
    }

    @Override
    public String getChunkName() {
      return chunkName;
    }

    @Override
    public int getTotalCount() {
      return totalCount;
    }

    public long getChecksum() {
      return checksum;
    }

    public byte[] getContent() {
      return content;
    }
  }
}
