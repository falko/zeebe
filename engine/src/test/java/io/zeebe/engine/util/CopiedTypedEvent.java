/*
 * Zeebe Workflow Engine
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
package io.zeebe.engine.util;

import io.zeebe.engine.processor.TypedEventImpl;
import io.zeebe.engine.processor.TypedRecord;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.zeebe.util.ReflectUtil;

public class CopiedTypedEvent extends TypedEventImpl {
  private final long key;
  private final long position;
  private final long sourcePosition;

  public CopiedTypedEvent(LoggedEvent event, UnifiedRecordValue object) {
    this.value = object;
    this.key = event.getKey();
    this.position = event.getPosition();
    this.sourcePosition = event.getSourceEventPosition();
    this.metadata = new RecordMetadata();
    event.readMetadata(metadata);
    event.readValue(object);
  }

  public CopiedTypedEvent(
      UnifiedRecordValue object,
      RecordMetadata recordMetadata,
      long key,
      long position,
      long sourcePosition) {
    this.metadata = recordMetadata;
    this.value = object;
    this.key = key;
    this.position = position;
    this.sourcePosition = sourcePosition;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public long getSourceRecordPosition() {
    return sourcePosition;
  }

  @Override
  public long getKey() {
    return key;
  }

  @Override
  public RecordMetadata getMetadata() {
    return metadata;
  }

  public static <T extends UnifiedRecordValue> TypedRecord<T> toTypedEvent(
      LoggedEvent event, Class<T> valueClass) {
    final T value = ReflectUtil.newInstance(valueClass);
    return new CopiedTypedEvent(event, value);
  }
}
