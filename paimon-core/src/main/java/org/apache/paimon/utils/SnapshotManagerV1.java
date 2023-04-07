/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public class SnapshotManagerV1 extends SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String EARLIEST = "EARLIEST";
    private static final String LATEST = "LATEST";

    public SnapshotManagerV1(FileIO fileIO, Path tablePath) {
        super(fileIO, tablePath);
    }

    @Override
    public String earliest() {
        return EARLIEST;
    }

    @Override
    public String latest() {
        return LATEST;
    }

    @Override
    public String snapshotPrefix() {
        return SNAPSHOT_PREFIX;
    }

    @Override
    public Path snapshotDirectory() {
        return new Path(tablePath + "/snapshot");
    }

    @Override
    public Path snapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    @Override
    public Path newSnapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    @Override
    public void expireSnapshot(long snapshotId) {
        fileIO.deleteQuietly(snapshotPath(snapshotId));
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return Snapshot.fromPath(fileIO, snapshotPath(snapshotId));
    }

    @Override
    public boolean snapshotExists(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    @Override
    public @Nullable Snapshot latestSnapshot() {
        Long snapshotId = latestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    @Override
    public @Nullable Long latestSnapshotId() {
        try {
            return findLatest();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    @Override
    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
    }

    @Override
    public @Nullable Long latestCompactedSnapshotId() {
        return pickSnapshot(s -> s.commitKind() == CommitKind.COMPACT);
    }

    @Override
    public @Nullable Long pickSnapshot(Predicate<Snapshot> predicate) {
        Long latestId = latestSnapshotId();
        Long earliestId = earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }

        for (long snapshotId = latestId; snapshotId >= earliestId; snapshotId--) {
            if (snapshotExists(snapshotId)) {
                Snapshot snapshot = snapshot(snapshotId);
                if (predicate.test(snapshot)) {
                    return snapshot.id();
                }
            }
        }

        return null;
    }

    @Override
    public @Nullable Long earlierThanTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        for (long i = latest; i >= earliest; i--) {
            long commitTime = snapshot(i).timeMillis();
            if (commitTime < timestampMills) {
                return i;
            }
        }
        return earliest - 1;
    }

    @Override
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        for (long i = latest; i >= earliest; i--) {
            Snapshot snapshot = snapshot(i);
            long commitTime = snapshot.timeMillis();
            if (commitTime <= timestampMills) {
                return snapshot;
            }
        }
        return null;
    }

    @Override
    public long snapshotCount() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX).count();
    }

    @Override
    public Iterator<Snapshot> snapshots() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    @Override
    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Optional.empty();
        }

        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");
        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    private @Nullable Long findLatest() throws IOException {
        Path snapshotDir = snapshotDirectory();
        if (!fileIO.exists(snapshotDir)) {
            return null;
        }

        Long snapshotId = readHint(LATEST);
        if (snapshotId != null) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!snapshotExists(nextSnapshot)) {
                return snapshotId;
            }
        }

        return findByListFiles(Math::max);
    }

    private @Nullable Long findEarliest() throws IOException {
        Path snapshotDir = snapshotDirectory();
        if (!fileIO.exists(snapshotDir)) {
            return null;
        }

        Long snapshotId = readHint(EARLIEST);
        // null and it is the earliest only it exists
        if (snapshotId != null && snapshotExists(snapshotId)) {
            return snapshotId;
        }

        return findByListFiles(Math::min);
    }

    @Override
    public Long readHint(String fileName) {
        Path snapshotDir = snapshotDirectory();
        Path path = new Path(snapshotDir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return Long.parseLong(fileIO.readFileUtf8(path));
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public Long readEarliestHint() {
        return readHint(EARLIEST);
    }

    @Override
    public Long readLatestHint() {
        return readHint(LATEST);
    }

    @Override
    public boolean commit(long newSnapshotId, Path newSnapshotPath, Snapshot newSnapshot)
            throws IOException {
        boolean committed = fileIO.writeFileUtf8(newSnapshotPath, newSnapshot.toJson());
        if (committed) {
            commitLatestHint(newSnapshotId);
        }
        return committed;
    }

    @Override
    public void commitLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST);
    }

    @Override
    public void commitEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST);
    }

    private void commitHint(long snapshotId, String fileName) throws IOException {
        Path snapshotDir = snapshotDirectory();
        Path hintFile = new Path(snapshotDir, fileName);
        fileIO.delete(hintFile, false);
        fileIO.writeFileUtf8(hintFile, String.valueOf(snapshotId));
    }
}
