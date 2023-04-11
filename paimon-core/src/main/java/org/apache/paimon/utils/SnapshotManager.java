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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public abstract class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final int READ_HINT_RETRY_NUM = 3;
    protected static final int READ_HINT_RETRY_INTERVAL = 1;

    protected final FileIO fileIO;
    protected final Path tablePath;

    public SnapshotManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public abstract String earliest();

    public abstract String latest();

    public abstract String snapshotPrefix();

    public abstract Path snapshotDirectory();

    public abstract Path snapshotPath(long snapshotId);

    public abstract Path newSnapshotPath(long snapshotId);

    public void expireSnapshot(long snapshotId) {
        fileIO.deleteQuietly(snapshotPath(snapshotId));
    }

    public abstract Snapshot snapshot(long snapshotId);

    public abstract boolean snapshotExists(long snapshotId);

    public abstract @Nullable Snapshot latestSnapshot();

    public abstract @Nullable Long latestSnapshotId();

    public abstract @Nullable Long earliestSnapshotId();

    public abstract @Nullable Long latestCompactedSnapshotId();

    public abstract @Nullable Long pickFromLatest(Predicate<Snapshot> predicate);

    /**
     * Returns a snapshot earlier than the timestamp mills. A non-existent snapshot may be returned
     * if all snapshots are later than the timestamp mills.
     */
    public abstract @Nullable Long earlierThanTimeMills(long timestampMills);

    /**
     * Returns a {@link Snapshot} whoes commit time is earlier than or equal to given timestamp
     * mills. If there is no such a snapshot, returns null.
     */
    public abstract @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills);

    public abstract long snapshotCount() throws IOException;

    public abstract Iterator<Snapshot> snapshots() throws IOException;

    public abstract Optional<Snapshot> latestSnapshotOfUser(String user);

    public abstract Long readHint(String fileName);

    public abstract Long readEarliestHint();

    public abstract Long readLatestHint();

    public abstract boolean commit(long newSnapshotId, Path newSnapshotPath, Snapshot newSnapshot)
            throws IOException;

    public abstract void commitLatestHint(long snapshotId) throws IOException;

    public abstract void commitEarliestHint(long snapshotId) throws IOException;

    protected Long findByListFiles(BinaryOperator<Long> reducer) throws IOException {
        Path snapshotDir = snapshotDirectory();
        return listVersionedFiles(fileIO, snapshotDir, snapshotPrefix())
                .reduce(reducer)
                .orElse(null);
    }
}
