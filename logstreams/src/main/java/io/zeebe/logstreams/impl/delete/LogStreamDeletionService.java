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
package io.zeebe.logstreams.impl.delete;

import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.impl.log.index.LogBlockIndex;
import io.zeebe.logstreams.impl.log.index.LogBlockIndexContext;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;

public abstract class LogStreamDeletionService implements DeletionService {
  protected LogStream logStream;
  protected final LogStorage logStorage;
  protected final LogBlockIndex logBlockIndex;
  protected final LogBlockIndexContext blockIndexContext;

  public LogStreamDeletionService(LogStream logStream) {
    logBlockIndex = logStream.getLogBlockIndex();
    logStorage = logStream.getLogStorage();
    blockIndexContext = logBlockIndex.createLogBlockIndexContext();
  }

  public void delete(long position) {
    position = Math.min(position, getMinimumExportedPosition());

    final long blockAddress = logBlockIndex.lookupBlockAddress(blockIndexContext, position);

    if (blockAddress != LogBlockIndex.VALUE_NOT_FOUND) {
      Loggers.LOGSTREAMS_LOGGER.info(
          "Delete data from logstream until position '{}' (address: '{}').",
          position,
          blockAddress);

      logBlockIndex.deleteUpToPosition(blockIndexContext, position);
      logStorage.delete(blockAddress);
    } else {
      Loggers.LOGSTREAMS_LOGGER.debug(
          "Tried to delete from log stream, but found no corresponding address in the log block index for the given position {}.",
          position);
    }
  }

  protected abstract long getMinimumExportedPosition();
}
