/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams.restore;

import io.zeebe.broker.engine.impl.SnapshotChunkImpl;
import io.zeebe.clustering.management.BooleanType;
import io.zeebe.clustering.management.SnapshotRestoreResponseDecoder;
import io.zeebe.clustering.management.SnapshotRestoreResponseEncoder;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreResponse;
import io.zeebe.distributedlog.restore.snapshot.impl.DefaultSnapshotRestoreResponse;
import io.zeebe.engine.util.SbeBufferWriterReader;
import io.zeebe.logstreams.state.SnapshotChunk;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class SbeSnapshotRestoreResponse
    extends SbeBufferWriterReader<SnapshotRestoreResponseEncoder, SnapshotRestoreResponseDecoder>
    implements SnapshotRestoreResponse {

  private final SnapshotRestoreResponseEncoder encoder;
  private final SnapshotRestoreResponseDecoder decoder;

  private final DefaultSnapshotRestoreResponse delegate;
  private DirectBuffer snapshotChunkBuffer;

  public SbeSnapshotRestoreResponse() {
    delegate = new DefaultSnapshotRestoreResponse();
    snapshotChunkBuffer = new UnsafeBuffer();
    encoder = new SnapshotRestoreResponseEncoder();
    decoder = new SnapshotRestoreResponseDecoder();
    reset();
  }

  public SbeSnapshotRestoreResponse(SnapshotRestoreResponse other) {
    this();
    setIsValid(other.isValid());
    delegate.setSnapshotChunk(other.getSnapshotChunk());
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    super.wrap(buffer, offset, length);
    setIsValid(decoder.isValid() == BooleanType.TRUE);
    decoder.wrapSnapshotChunk(snapshotChunkBuffer);
    setSnapshotChunk(snapshotChunkBuffer);
  }

  private void setSnapshotChunk(DirectBuffer snapshotChunkBuffer) {
    final SnapshotChunkImpl snapshotChunk = new SnapshotChunkImpl();
    snapshotChunk.wrap(snapshotChunkBuffer);
    delegate.setSnapshotChunk(snapshotChunk);
  }

  private void setIsValid(boolean isValid) {
    delegate.setIsValid(isValid);
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    super.write(buffer, offset);
    encoder.isValid(delegate.isValid() ? BooleanType.TRUE : BooleanType.FALSE);
    if (delegate.isValid()) {
      final SnapshotChunkImpl chunk = new SnapshotChunkImpl(delegate.getSnapshotChunk());
      encoder.putSnapshotChunk(chunk.toBytes(), 0, chunk.getLength());
    } else {
      // TODO: how to handle error response
    }
  }

  @Override
  public SnapshotChunk getSnapshotChunk() {
    return delegate.getSnapshotChunk();
  }

  @Override
  public boolean isValid() {
    return delegate.isValid();
  }

  public static byte[] serialize(SnapshotRestoreResponse response) {
    return new SbeSnapshotRestoreResponse(response).toBytes();
  }

  @Override
  protected SnapshotRestoreResponseEncoder getBodyEncoder() {
    return encoder;
  }

  @Override
  protected SnapshotRestoreResponseDecoder getBodyDecoder() {
    return decoder;
  }
}
