/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuard.FileVisibility;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.rdd.HoodieRDDBloomIndexV2;
import org.apache.hudi.common.rdd.HoodieRDDCopyOnWriteTable;
import org.apache.hudi.common.rdd.HoodieRDDMergeOnReadTable;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.commit.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Abstract implementation of a HoodieTable.
 */
public abstract class HoodieTable<T extends HoodieRecordPayload, INPUT extends HoodieWriteInput, KEY extends HoodieWriteKey, OUTPUT extends HoodieWriteOutput> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTable.class);

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieIndexV2 index;

  private SerializableConfiguration hadoopConfiguration;
  private transient FileSystemViewManager viewManager;

  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  protected HoodieTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, Configuration configuration) {
    this.config = config;
    this.hadoopConfiguration = new SerializableConfiguration(configuration);
    this.viewManager = FileSystemViewManager.createViewManager(new SerializableConfiguration(configuration),
        config.getViewStorageConfig());
    this.metaClient = metaClient;
    this.index = new HoodieRDDBloomIndexV2<T>();
  }

  private synchronized FileSystemViewManager getViewManager() {
    if (null == viewManager) {
      viewManager = FileSystemViewManager.createViewManager(hadoopConfiguration, config.getViewStorageConfig());
    }
    return viewManager;
  }

  public static <T extends HoodieRecordPayload,
          INPUT extends HoodieWriteInput,
          KEY extends HoodieWriteKey,
          OUTPUT extends HoodieWriteOutput> HoodieTable<T, INPUT, KEY, OUTPUT> create(HoodieWriteConfig config) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(
            new Configuration(),
        config.getBasePath(),
        true,
        config.getConsistencyGuardConfig(),
        Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))
    );
    return HoodieTable.create(metaClient, config);
  }

  public static <T extends HoodieRecordPayload,
          INPUT extends HoodieWriteInput,
          KEY extends HoodieWriteKey,
          OUTPUT extends HoodieWriteOutput> HoodieTable<T, INPUT, KEY, OUTPUT> create(HoodieTableMetaClient metaClient,
                                                                      HoodieWriteConfig config) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieRDDCopyOnWriteTable(config, metaClient);
      case MERGE_ON_READ:
        return new HoodieRDDMergeOnReadTable(config, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   * @param instantTime Instant Time for the action
   * @param records  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata upsert(String instantTime, INPUT records);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   * @param instantTime Instant Time for the action
   * @param records  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata insert(String instantTime, INPUT records);

  /**
   * Bulk Insert a batch of new records into Hoodie table at the supplied instantTime.
   * @param instantTime Instant Time for the action
   * @param records  JavaRDD of hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata bulkInsert(String instantTime,
      INPUT records, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * @param instantTime Instant Time for the action
   * @param keys   {@link List} of {@link HoodieKey}s to be deleted
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata delete(String instantTime, KEY keys);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param instantTime Instant Time for the action
   * @param preppedRecords  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata upsertPrepped(String instantTime,
      INPUT preppedRecords);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param instantTime Instant Time for the action
   * @param preppedRecords  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata insertPrepped(String instantTime,
      INPUT preppedRecords);

  /**
   * Bulk Insert the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param instantTime Instant Time for the action
   * @param preppedRecords  JavaRDD of hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata bulkInsertPrepped(String instantTime,
      INPUT preppedRecords,  Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner);

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Configuration getHadoopConf() {
    return metaClient.getHadoopConf();
  }

  /**
   * Get the view of the file system for this table.
   */
  public TableFileSystemView getFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitsTimeline());
  }

  /**
   * Get the base file only view of the file system for this table.
   */
  public BaseFileOnlyView getBaseFileOnlyView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get the full view of the file system for this table.
   */
  public SliceView getSliceView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get complete view of the file system for this table with ability to force sync.
   */
  public SyncableFileSystemView getHoodieView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get only the completed (no-inflights) commit + deltacommit timeline.
   */
  public HoodieTimeline getCompletedCommitsTimeline() {
    return metaClient.getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Get only the completed (no-inflights) commit timeline.
   */
  public HoodieTimeline getCompletedCommitTimeline() {
    return metaClient.getCommitTimeline().filterCompletedInstants();
  }

  /**
   * Get only the inflights (no-completed) commit timeline.
   */
  public HoodieTimeline getPendingCommitTimeline() {
    return metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
  }

  /**
   * Get only the completed (no-inflights) clean timeline.
   */
  public HoodieTimeline getCompletedCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline().filterCompletedInstants();
  }

  /**
   * Get clean timeline.
   */
  public HoodieTimeline getCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline();
  }

  /**
   * Get only the completed (no-inflights) savepoint timeline.
   */
  public HoodieTimeline getCompletedSavepointTimeline() {
    return getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
  }

  /**
   * Get the list of savepoints in this table.
   */
  public List<String> getSavepoints() {
    return getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  /**
   * Get the list of data file names savepointed.
   */
  public Stream<String> getSavepointedDataFiles(String savepointTime) {
    if (!getSavepoints().contains(savepointTime)) {
      throw new HoodieSavepointException(
          "Could not get data files for savepoint " + savepointTime + ". No such savepoint.");
    }
    HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    HoodieSavepointMetadata metadata;
    try {
      metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(getActiveTimeline().getInstantDetails(instant).get());
    } catch (IOException e) {
      throw new HoodieSavepointException("Could not get savepointed data files for savepoint " + savepointTime, e);
    }
    return metadata.getPartitionMetadata().values().stream().flatMap(s -> s.getSavepointDataFile().stream());
  }

  public HoodieActiveTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline();
  }

  /**
   * Return the index.
   */
  public HoodieIndexV2 getIndex() {
    return index;
  }

  /**
   * Schedule compaction for the instant time.
   * 
   * @param instantTime Instant Time for scheduling compaction
   * @return
   */
  public abstract HoodieCompactionPlan scheduleCompaction(String instantTime);

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data access.
   *
   * @param compactionInstantTime Instant Time
   * @param compactionPlan Compaction Plan
   */
  public abstract OUTPUT compact(String compactionInstantTime,
      HoodieCompactionPlan compactionPlan);

  /**
   * Executes a new clean action.
   *
   * @return information on cleaned file slices
   */
  public abstract HoodieCleanMetadata clean(String cleanInstantTime);

  /**
   * Rollback the (inflight/committed) record changes with the given commit time.
   * <pre>
   *   Three steps:
   *   (1) Atomically unpublish this commit
   *   (2) clean indexing data
   *   (3) clean new generated parquet files.
   *   (4) Finally delete .commit or .inflight file, if deleteInstants = true
   * </pre>
   */
  public abstract HoodieRollbackMetadata rollback(String rollbackInstantTime,
                                                  HoodieInstant commitInstant,
                                                  boolean deleteInstants);

  /**
   * Restore the table to the given instant. Note that this is a admin table recovery operation
   * that would cause any running queries that are accessing file slices written after the instant to fail.
   */
  public abstract HoodieRestoreMetadata restore(String restoreInstantTime,
                                                String instantToRestore);

  /**
   * Finalize the written data onto storage. Perform any final cleanups.
   *
   * @param stats List of HoodieWriteStats
   * @throws HoodieIOException if some paths can't be finalized on storage
   */
  public void finalizeWrite(String instantTs, List<HoodieWriteStat> stats) throws HoodieIOException {
    cleanFailedWrites(instantTs, stats, config.getConsistencyGuardConfig().isConsistencyCheckEnabled());
  }

  /**
   * Delete Marker directory corresponding to an instant.
   * 
   * @param instantTs Instant Time
   */
  public void deleteMarkerDir(String instantTs) {
    try {
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));
      if (fs.exists(markerDir)) {
        // For append only case, we do not write to marker dir. Hence, the above check
        LOG.info("Removing marker directory=" + markerDir);
        fs.delete(markerDir, true);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  public abstract void cleanFailedWrites(String instantTs, List<HoodieWriteStat> stats,
                                              boolean consistencyCheckEnabled) throws HoodieIOException;

  /**
   * Ensures all files passed either appear or disappear.
   * 
   * @param groupByPartition Files grouped by partition
   * @param visibility Appear/Disappear
   */
  public abstract void waitForAllFiles(Map<String, List<Pair<String, String>>> groupByPartition, FileVisibility visibility);

  public SparkTaskContextSupplier getSparkTaskContextSupplier() {
    return sparkTaskContextSupplier;
  }
}
