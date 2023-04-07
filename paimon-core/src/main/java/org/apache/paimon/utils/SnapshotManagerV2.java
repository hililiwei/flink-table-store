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
import org.apache.paimon.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * Manager for {@link Snapshot} with {@link TableMetadata}, providing utility methods related to
 * paths and snapshot hints.
 */
public class SnapshotManagerV2 extends SnapshotManager {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManagerV2.class);

    private static final long serialVersionUID = 1L;

    private static final String TABLE_METADATA_PREFIX = "metadata-";
    private static final String METADATA_LATEST = "METADATA_LATEST";
    private static final String METADATA_EARLIEST = "METADATA_EARLIEST";

    private final SnapshotManagerV1 superManager;
    private boolean useV2 = true;

    public SnapshotManagerV2(FileIO fileIO, Path tablePath) {
        super(fileIO, tablePath);
        superManager = new SnapshotManagerV1(fileIO, tablePath);
        try {
            if (!metadataExists() && superManager.snapshotCount() > 0) {
                upgradeTable();
            }
        } catch (IOException e) {
            LOG.warn("Failed to upgrade table", e);
        }
    }

    public String earliest() {
        return METADATA_EARLIEST;
    }

    public String latest() {
        return METADATA_LATEST;
    }

    @Override
    public String snapshotPrefix() {
        return TABLE_METADATA_PREFIX;
    }

    private Optional<Long> findLatestMetadataId() throws IOException {
        Path metadataDirectory = tableMetadataDirectory(tablePath);
        if (!fileIO.exists(metadataDirectory)) {
            return Optional.empty();
        }

        Long metadataId = readMetadataHint(METADATA_LATEST);
        if (metadataId != null) {
            long nextId = metadataId + 1;
            // it is the latest only there is no next one
            if (!medataExists(nextId)) {
                return Optional.of(metadataId);
            }
        }

        return Optional.of(findByListFiles(Math::max));
    }

    private Optional<TableMetadata> findLatestMetadata() {
        try {
            return findLatestMetadataId().map(id -> tableMetadata(fileIO, metadataPath(id)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest metadata", e);
        }
    }

    @Override
    public Path snapshotDirectory() {
        return new Path(tablePath + "/snapshot");
    }

    private void upgradeTable() {
        List<Snapshot> snapshots;
        try {
            List<Snapshot> snapshotHis = Lists.newArrayList(superManager.snapshots());
            snapshots = Lists.newArrayList();
            Long parentId = null;
            for (Snapshot snapshot : snapshotHis) {
                snapshots.add(snapshot.copyWithParentId(parentId));
                parentId = snapshot.id();
            }

            Snapshot currentSnapshot = snapshots.get(snapshots.size() - 1);
            TableMetadata newTableMetadata =
                    new TableMetadata(
                            currentSnapshot.timeMillis(),
                            Maps.newHashMap(),
                            currentSnapshot.id(),
                            snapshots);

            long currentSnapshotId = currentSnapshot.id();

            boolean committed =
                    fileIO.writeFileUtf8(
                            metadataPath(currentSnapshotId), newTableMetadata.toJson());
            if (committed) {
                commitLatestHint(currentSnapshotId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to upgrade table", e);
        }
    }

    private TableMetadata generateLatestMetadata(Snapshot newSnapshot) {
        List<Snapshot> snapshots = Lists.newArrayList();
        Optional<TableMetadata> latestMetadata = findLatestMetadata();
        if (latestMetadata.isPresent()) {
            TableMetadata tableMetadata = latestMetadata.get();
            snapshots = tableMetadata.snapshots();
        } else {
            try {
                List<Snapshot> snapshotHis = Lists.newArrayList(snapshots());
                Long parentId = null;
                for (Snapshot snapshot : snapshotHis) {
                    snapshots.add(snapshot.copyWithParentId(parentId));
                    parentId = snapshot.id();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to generate latest metadata", e);
            }
        }

        snapshots.add(newSnapshot);

        return new TableMetadata(
                newSnapshot.timeMillis(), Maps.newHashMap(), newSnapshot.id(), snapshots);
    }

    private Optional<Snapshot> pickLatestSnapshotFromMetadata(Predicate<Snapshot> predicate) {
        Snapshot snapshot = Iterables.getFirst(select(currentAncestors(), predicate), null);
        if (snapshot != null) {
            return Optional.of(snapshot);
        }

        return Optional.empty();
    }

    public static Path tableMetadataDirectory(Path tablePath) {
        return new Path(tablePath + "/snapshot");
    }

    @Override
    public Path snapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + TABLE_METADATA_PREFIX + snapshotId);
    }

    @Override
    public Path newSnapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + TABLE_METADATA_PREFIX + snapshotId);
    }

    public static TableMetadata tableMetadata(FileIO fileIO, Path path) {
        return TableMetadata.fromPath(fileIO, path);
    }

    private void commitMetadataHint(long metadataId, String fileName) throws IOException {
        Path snapshotDir = tableMetadataDirectory(tablePath);
        Path hintFile = new Path(snapshotDir, fileName);
        fileIO.delete(hintFile, false);
        fileIO.writeFileUtf8(hintFile, String.valueOf(metadataId));
    }

    private Path metadataPath(long metadataId) {
        return new Path(tablePath + "/snapshot/" + TABLE_METADATA_PREFIX + metadataId);
    }

    @Override
    public void expireSnapshot(long snapshotId) {
        Optional<TableMetadata> latestMetadata = findLatestMetadata();
        if (!latestMetadata.isPresent()) {
            throw new RuntimeException("Failed to find latest metadata");
        }

        // todo Conflict checking should be done here
        TableMetadata tableMetadata = latestMetadata.get();
        Snapshot currentSnapshot = latestSnapshot();
        if (snapshotId == currentSnapshot.id()) {
            Long parentId = currentSnapshot.parentId();
            if (parentId != null) {
                currentSnapshot = snapshot(parentId);
            }
        }

        List<Snapshot> snapshots = tableMetadata.snapshots();
        snapshots.removeIf(s -> s.id() == snapshotId);

        // todo This should be done in conjunction with the snapshot commit, not separately as is
        // currently the case.
        TableMetadata newTableMetadata =
                new TableMetadata(
                        currentSnapshot.timeMillis(),
                        Maps.newHashMap(),
                        currentSnapshot.id(),
                        snapshots);

        long currentSnapshotId = currentSnapshot.id();
        try {
            // todo There is a consistency issuse.
            fileIO.delete(metadataPath(snapshotId), false);

            LOG.debug(
                    "Committing snapshot id: {}, metadata: {}",
                    currentSnapshotId,
                    newTableMetadata.toJson());
            fileIO.delete(metadataPath(currentSnapshotId), false);
            boolean committed =
                    fileIO.writeFileUtf8(
                            metadataPath(currentSnapshotId), newTableMetadata.toJson());
            if (committed) {
                commitLatestHint(currentSnapshotId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit metadata: " + snapshotId, e);
        }
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        if (useSuper()) {
            return superManager.snapshot(snapshotId);
        }

        TableMetadata tableMetadata;
        try {
            Optional<TableMetadata> latestMetadata = findLatestMetadata();
            if (latestMetadata.isPresent()) {
                tableMetadata = latestMetadata.get();
            } else {
                tableMetadata = tableMetadata(fileIO, metadataPath(findByListFiles(Math::max)));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest metadata", e);
        }

        return tableMetadata.snapshots().stream()
                .filter(s -> s.id() == snapshotId)
                .findAny()
                .orElse(null);
    }

    @Override
    public boolean snapshotExists(long snapshotId) {
        if (useSuper()) {
            return superManager.snapshotExists(snapshotId);
        }

        return findLatestMetadata()
                .flatMap(t -> t.snapshots().stream().filter(s -> s.id() == snapshotId).findAny())
                .isPresent();
    }

    private boolean medataExists(long medataId) throws IOException {
        return fileIO.exists(metadataPath(medataId));
    }

    @Override
    public @Nullable Snapshot latestSnapshot() {
        if (useSuper()) {
            return superManager.latestSnapshot();
        }

        return findLatestMetadata()
                .flatMap(
                        tableMetadata ->
                                tableMetadata.snapshots().stream()
                                        .filter(s -> s.id() == tableMetadata.currentSnapshotId())
                                        .findAny())
                .orElse(null);
    }

    @Override
    public @Nullable Long latestSnapshotId() {
        if (useSuper()) {
            return superManager.latestSnapshotId();
        }

        try {
            Optional<Long> currentSnapshotId =
                    findLatestMetadata().map(TableMetadata::currentSnapshotId);
            if (currentSnapshotId.isPresent()) {
                return currentSnapshotId.get();
            }

            return findByListFiles(Math::max);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    @Override
    public @Nullable Long earliestSnapshotId() {
        if (useSuper()) {
            return superManager.earliestSnapshotId();
        }

        Optional<Snapshot> lastSnapshot = Optional.empty();
        for (Snapshot snapshot : currentAncestors()) {
            lastSnapshot = Optional.of(snapshot);
        }

        Optional<Snapshot> earliestMetadata = lastSnapshot;
        return earliestMetadata.map(Snapshot::id).orElse(null);
    }

    @Override
    public @Nullable Long latestCompactedSnapshotId() {
        return pickSnapshot(s -> s.commitKind() == CommitKind.COMPACT);
    }

    @Override
    public @Nullable Long pickSnapshot(Predicate<Snapshot> predicate) {
        if (useSuper()) {
            return superManager.pickSnapshot(predicate);
        }

        Optional<Snapshot> pickLatestSnapshot = pickLatestSnapshotFromMetadata(predicate);
        return pickLatestSnapshot.map(Snapshot::id).orElse(null);
    }

    @Override
    public @Nullable Long earlierThanTimeMills(long timestampMills) {
        if (useSuper()) {
            return superManager.earlierThanTimeMills(timestampMills);
        }

        Snapshot lastSnapshot = null;
        for (Snapshot snapshot : currentAncestors()) {
            if (snapshot.timeMillis() < timestampMills) {
                return snapshot.id();
            }

            lastSnapshot = snapshot;
        }

        if (lastSnapshot != null && lastSnapshot.parentId() == null) {
            // this is the first snapshot in the table
            return lastSnapshot.id() - 1;
        }

        return null;
    }

    @Override
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        if (useSuper()) {
            return superManager.earlierOrEqualTimeMills(timestampMills);
        }

        for (Snapshot snapshot : currentAncestors()) {
            if (snapshot.timeMillis() <= timestampMills) {
                return snapshot;
            }
        }

        return null;
    }

    @Override
    public long snapshotCount() throws IOException {
        if (useSuper()) {
            return superManager.snapshotCount();
        }

        Optional<TableMetadata> tableMetadata = findLatestMetadata();
        return tableMetadata
                .orElseThrow(() -> new RuntimeException("Failed to find latest metadata"))
                .snapshots()
                .size();
    }

    @Override
    public Iterator<Snapshot> snapshots() throws IOException {
        if (useSuper()) {
            return superManager.snapshots();
        }

        return StreamSupport.stream(currentAncestors().spliterator(), false)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    @Override
    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        if (useSuper()) {
            return superManager.latestSnapshotOfUser(user);
        }

        return pickLatestSnapshotFromMetadata(s -> user.equals(s.commitUser()));
    }

    @Override
    public Long readHint(String fileName) {
        Long hint = readMetadataHint(fileName);
        if (hint == null) {
            return superManager.readHint(fileName);
        }

        return hint;
    }

    @Override
    public Long readEarliestHint() {
        return readHint(METADATA_EARLIEST);
    }

    @Override
    public Long readLatestHint() {
        return readHint(METADATA_LATEST);
    }

    @Override
    public void commitLatestHint(long snapshotId) throws IOException {
        commitMetadataHint(snapshotId, METADATA_LATEST);
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitMetadataHint(snapshotId, METADATA_EARLIEST);
    }

    @Override
    public boolean commit(long newSnapshotId, Path newSnapshotPath, Snapshot newSnapshot)
            throws IOException {
        TableMetadata newTableMetadata = generateLatestMetadata(newSnapshot);

        boolean committed = fileIO.writeFileUtf8(newSnapshotPath, newTableMetadata.toJson());
        if (committed) {
            commitLatestHint(newSnapshotId);
            useV2 = true;
        }
        return committed;
    }

    private static <T> Iterable<T> select(Iterable<T> it, Predicate<T> pred) {
        return () -> StreamSupport.stream(it.spliterator(), false).filter(pred).iterator();
    }

    public Iterable<Snapshot> currentAncestors() {
        return ancestorsOf(latestSnapshot(), this::snapshot);
    }

    private static Iterable<Snapshot> ancestorsOf(
            Snapshot snapshot, Function<Long, Snapshot> lookup) {
        if (snapshot != null) {
            return () ->
                    new Iterator<Snapshot>() {
                        private Snapshot next = snapshot;
                        private boolean consumed = false; // include the snapshot in its history

                        @Override
                        public boolean hasNext() {
                            if (!consumed) {
                                return true;
                            }

                            Long parentId = next.parentId();
                            if (parentId == null) {
                                return false;
                            }

                            this.next = lookup.apply(parentId);
                            if (next != null) {
                                this.consumed = false;
                                return true;
                            }

                            return false;
                        }

                        @Override
                        public Snapshot next() {
                            if (hasNext()) {
                                this.consumed = true;
                                return next;
                            }

                            throw new NoSuchElementException();
                        }
                    };
        } else {
            return ImmutableList.of();
        }
    }

    private Long readMetadataHint(String fileName) {
        Path snapshotDir = tableMetadataDirectory(tablePath);
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

    private boolean useSuper() {
        return false;
    }

    private boolean metadataExists() {
        Path snapshotDir = tableMetadataDirectory(tablePath);
        Path path = new Path(snapshotDir, METADATA_LATEST);
        try {
            if (fileIO.exists(path)) {
                return true;
            }
        } catch (IOException e) {
            LOG.warn("Failed to check metadata hint exist or not", e);
        }
        return false;
    }
}
