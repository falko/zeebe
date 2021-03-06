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
package io.zeebe.broker.exporter.record.value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.zeebe.broker.exporter.ExporterObjectMapper;
import io.zeebe.broker.exporter.record.RecordValueImpl;
import io.zeebe.exporter.api.record.value.VariableDocumentRecordValue;
import io.zeebe.protocol.VariableDocumentUpdateSemantic;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class VariableDocumentRecordValueImpl extends RecordValueImpl
    implements VariableDocumentRecordValue {

  private final long scopeKey;
  private final VariableDocumentUpdateSemantic updateSemantics;

  @JsonIgnore private final Supplier<Map<String, Object>> documentSupplier;
  @JsonIgnore private Map<String, Object> document;

  public VariableDocumentRecordValueImpl(
      ExporterObjectMapper objectMapper,
      long scopeKey,
      VariableDocumentUpdateSemantic updateSemantics,
      Supplier<Map<String, Object>> documentSupplier) {
    super(objectMapper);
    this.scopeKey = scopeKey;
    this.updateSemantics = updateSemantics;
    this.documentSupplier = documentSupplier;
  }

  @Override
  public long getScopeKey() {
    return scopeKey;
  }

  @Override
  public VariableDocumentUpdateSemantic getUpdateSemantics() {
    return updateSemantics;
  }

  @Override
  @JsonProperty
  public Map<String, Object> getDocument() {
    if (document == null) {
      document = documentSupplier.get();
    }
    return document;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof VariableDocumentRecordValueImpl)) {
      return false;
    }

    final VariableDocumentRecordValueImpl that = (VariableDocumentRecordValueImpl) o;
    return getScopeKey() == that.getScopeKey()
        && getUpdateSemantics() == that.getUpdateSemantics()
        && Objects.equals(getDocument(), that.getDocument());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getScopeKey(), getUpdateSemantics(), getDocument());
  }

  @Override
  public String toString() {
    return "VariableDocumentRecordValueImpl{"
        + "scopeKey="
        + scopeKey
        + ", updateSemantics="
        + updateSemantics
        + ", document="
        + getDocument()
        + "} "
        + super.toString();
  }
}
