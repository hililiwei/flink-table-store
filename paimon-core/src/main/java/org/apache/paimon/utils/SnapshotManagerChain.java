/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.Serializable;

/** Builder for {@link SnapshotManager}. */
public final class SnapshotManagerChain implements Serializable {

    private static final long serialVersionUID = 1L;

    private FileIO fileIO;
    private Path tablePath;

    private SnapshotManagerChain() {}

    private SnapshotManagerChain(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public static SnapshotManager of(FileIO fileIO, Path tablePath) {
        return new SnapshotManagerChain(fileIO, tablePath).build();
    }

    private SnapshotManager build() {
        return new SnapshotManagerV2(fileIO, tablePath);
    }
}
