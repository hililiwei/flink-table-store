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

public class TableMetadata {

    private static final int CURRENT_VERSION = 1;

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_LOCATION = "location";
    private static final String FIELD_LAST_UPDATED_MS = "last-updated-ms";
    private static final String FIELD_PROPERTIES = "properties";
    private static final String FIELD_CURRENT_SNAPSHOT_ID = "current-snapshot-id";
    private static final String FIELD_SNAPSHOTS = "snapshots";
    private static final String FIELD_SNAPSHOT_LOG = "snapshot-log";
    private static final String FIELD_METADATA_LOG = "metadata-log";

    @JsonProperty(FIELD_VERSION)
    private final Integer version;

    @JsonProperty(FIELD_LOCATION)
    private final String location;

    @JsonProperty(FIELD_LAST_UPDATED_MS)
    private final Long lastUpdatedMs;

    @JsonProperty(FIELD_PROPERTIES)
    private final Map<String, String> properties;

    @JsonProperty(FIELD_CURRENT_SNAPSHOT_ID)
    private final Integer currentSnapshotId;

    @JsonProperty(FIELD_SNAPSHOTS)
    private final List<Snapshot> snapshots;

    @JsonProperty(FIELD_SNAPSHOT_LOG)
    private final List<SnapshotLog> snapshotLog;

    @JsonProperty(FIELD_METADATA_LOG)
    private final List<MetadataLog> metadataLog;

    public TableMetadata(
            String location,
            Long lastUpdatedMs,
            Map<String, String> properties,
            Integer currentSnapshotId,
            List<Snapshot> snapshots,
            List<SnapshotLog> snapshotLog,
            List<MetadataLog> metadataLog) {
        this(
                CURRENT_VERSION,
                location,
                lastUpdatedMs,
                properties,
                currentSnapshotId,
                snapshots,
                snapshotLog,
                metadataLog);
    }

    @JsonCreator
    public TableMetadata(
            @JsonProperty(FIELD_VERSION) int version,
            @JsonProperty(FIELD_LOCATION) String location,
            @JsonProperty(FIELD_LAST_UPDATED_MS) Long lastUpdatedMs,
            @JsonProperty(FIELD_PROPERTIES) Map<String, String> properties,
            @JsonProperty(FIELD_CURRENT_SNAPSHOT_ID) Integer currentSnapshotId,
            @JsonProperty(FIELD_SNAPSHOTS) List<Snapshot> snapshots,
            @JsonProperty(FIELD_SNAPSHOT_LOG) List<SnapshotLog> snapshotLog,
            @JsonProperty(FIELD_METADATA_LOG) List<MetadataLog> metadataLog) {
        this.version = version;
        this.location = location;
        this.lastUpdatedMs = lastUpdatedMs;
        this.properties = properties;
        this.currentSnapshotId = currentSnapshotId;
        this.snapshots = snapshots;
        this.snapshotLog = snapshotLog;
        this.metadataLog = metadataLog;
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        return version;
    }

    @JsonGetter(FIELD_LOCATION)
    public String location() {
        return location;
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
    public Integer currentSnapshotId() {
        return currentSnapshotId;
    }

    @JsonGetter(FIELD_SNAPSHOTS)
    public List<Snapshot> snapshots() {
        return snapshots;
    }

    @JsonGetter(FIELD_SNAPSHOT_LOG)
    public List<SnapshotLog> snapshotLog() {
        return snapshotLog;
    }

    @JsonGetter(FIELD_METADATA_LOG)
    public List<MetadataLog> metadataLog() {
        return metadataLog;
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

class MetadataLog {
    private static final String FIELD_TIMESTAMP_MS = "timestamp-ms";
    private static final String FIELD_METADATA_FILE = "metadata-file";

    @JsonProperty(FIELD_TIMESTAMP_MS)
    private final Long timestampMs;

    @JsonProperty(FIELD_METADATA_FILE)
    private final String metadataFile;

    @JsonCreator
    public MetadataLog(
            @JsonProperty(FIELD_TIMESTAMP_MS) Long timestampMs,
            @JsonProperty(FIELD_METADATA_FILE) String metadataFile) {
        this.timestampMs = timestampMs;
        this.metadataFile = metadataFile;
    }

    @JsonGetter(FIELD_TIMESTAMP_MS)
    public Long timestampMs() {
        return timestampMs;
    }

    @JsonGetter(FIELD_METADATA_FILE)
    public String metadataFile() {
        return metadataFile;
    }
}

class SnapshotLog {
    private static final String FIELD_TIMESTAMP_MS = "timestamp-ms";
    private static final String FIELD_SNAPSHOT_ID = "snapshot-id";

    @JsonProperty(FIELD_TIMESTAMP_MS)
    private final Long timestampMs;

    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final Long snapshotId;

    @JsonCreator
    public SnapshotLog(
        @JsonProperty(FIELD_TIMESTAMP_MS) Long timestampMs,
        @JsonProperty(FIELD_SNAPSHOT_ID) Long snapshotId) {
        this.timestampMs = timestampMs;
        this.snapshotId = snapshotId;
    }

    @JsonGetter(FIELD_TIMESTAMP_MS)
    public Long timestampMs() {
        return timestampMs;
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    public Long snapshotId() {
        return snapshotId;
    }
}
