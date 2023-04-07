/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Table metadata. This class is immutable. It is used to store the metadata of a table. */
public class TableMetadata {

    private static final Integer CURRENT_VERSION = 1;

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_LAST_UPDATED_MS = "last-updated-ms";
    private static final String FIELD_PROPERTIES = "properties";
    private static final String FIELD_CURRENT_SNAPSHOT_ID = "current-snapshot-id";
    private static final String FIELD_SNAPSHOTS = "snapshots";

    @JsonProperty(FIELD_VERSION)
    private final Integer version;

    @JsonProperty(FIELD_LAST_UPDATED_MS)
    private final Long lastUpdatedMs;

    @JsonProperty(FIELD_PROPERTIES)
    private final Map<String, String> properties;

    @JsonProperty(FIELD_CURRENT_SNAPSHOT_ID)
    private final Long currentSnapshotId;

    @JsonProperty(FIELD_SNAPSHOTS)
    private final List<Snapshot> snapshots;

    public TableMetadata(
            Long lastUpdatedMs,
            Map<String, String> properties,
            Long currentSnapshotId,
            List<Snapshot> snapshots) {
        this(CURRENT_VERSION, lastUpdatedMs, properties, currentSnapshotId, snapshots);
    }

    @JsonCreator
    public TableMetadata(
            @JsonProperty(FIELD_VERSION) Integer version,
            @JsonProperty(FIELD_LAST_UPDATED_MS) Long lastUpdatedMs,
            @JsonProperty(FIELD_PROPERTIES) Map<String, String> properties,
            @JsonProperty(FIELD_CURRENT_SNAPSHOT_ID) Long currentSnapshotId,
            @JsonProperty(FIELD_SNAPSHOTS) List<Snapshot> snapshots) {
        this.version = version;
        this.lastUpdatedMs = lastUpdatedMs;
        this.properties = properties;
        this.currentSnapshotId = currentSnapshotId;
        this.snapshots = snapshots;
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        return version;
    }

    @JsonGetter(FIELD_LAST_UPDATED_MS)
    public Long lastUpdatedMs() {
        return lastUpdatedMs;
    }

    @JsonGetter(FIELD_PROPERTIES)
    public Map<String, String> properties() {
        return properties;
    }

    @JsonGetter(FIELD_CURRENT_SNAPSHOT_ID)
    public Long currentSnapshotId() {
        return currentSnapshotId;
    }

    @JsonGetter(FIELD_SNAPSHOTS)
    public List<Snapshot> snapshots() {
        return snapshots;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static TableMetadata fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, TableMetadata.class);
    }

    public static TableMetadata fromPath(FileIO fileIO, Path path) {
        try {
            String json = fileIO.readFileUtf8(path);
            return TableMetadata.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Fails to read snapshot from path " + path, e);
        }
    }
}
