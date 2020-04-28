package org.apache.hudi.common.rdd;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieTable;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class HoodieRDDTable<T extends HoodieRecordPayload>
    extends HoodieTable<T, HoodieWriteInput<JavaRDD<HoodieRecord<T>>>, HoodieWriteKey<JavaRDD<HoodieKey>>, HoodieWriteOutput<JavaRDD<WriteStatus>>> {
  private static final Logger LOG = LogManager.getLogger(HoodieRDDTable.class);

  protected JavaSparkContext jsc;

  public HoodieRDDTable(JavaSparkContext jsc, HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient, jsc.hadoopConfiguration());
    this.jsc = jsc;
  }

  @Override
  public void waitForAllFiles(Map<String, List<Pair<String, String>>> groupByPartition, ConsistencyGuard.FileVisibility visibility) {
    // This will either ensure all files to be deleted are present.
    boolean checkPassed =
            jsc.parallelize(new ArrayList<>(groupByPartition.entrySet()), config.getFinalizeWriteParallelism())
                    .map(partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
                            partitionWithFileList.getValue().stream(), visibility))
                    .collect().stream().allMatch(x -> x);
    if (!checkPassed) {
      throw new HoodieIOException("Consistency check failed to ensure all files " + visibility);
    }
  }

  private boolean waitForCondition(String partitionPath, Stream<Pair<String, String>> partitionFilePaths, ConsistencyGuard.FileVisibility visibility) {
    final FileSystem fileSystem = metaClient.getRawFs();
    List<String> fileList = partitionFilePaths.map(Pair::getValue).collect(Collectors.toList());
    try {
      getFailSafeConsistencyGuard(fileSystem).waitTill(partitionPath, fileList, visibility);
    } catch (IOException | TimeoutException ioe) {
      LOG.error("Got exception while waiting for files to show up", ioe);
      return false;
    }
    return true;
  }

  private ConsistencyGuard getFailSafeConsistencyGuard(FileSystem fileSystem) {
    return new FailSafeConsistencyGuard(fileSystem, config.getConsistencyGuardConfig());
  }

  /**
   * Reconciles WriteStats and marker files to detect and safely delete duplicate data files created because of Spark
   * retries.
   *
   * @param instantTs Instant Timestamp
   * @param stats Hoodie Write Stat
   * @param consistencyCheckEnabled Consistency Check Enabled
   * @throws HoodieIOException
   */
  @Override
  protected void cleanFailedWrites(String instantTs, List<HoodieWriteStat> stats,
                                   boolean consistencyCheckEnabled) throws HoodieIOException {
    try {
      // Reconcile marker and data files with WriteStats so that partially written data-files due to failed
      // (but succeeded on retry) tasks are removed.
      String basePath = getMetaClient().getBasePath();
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));

      if (!fs.exists(markerDir)) {
        // Happens when all writes are appends
        return;
      }

      List<String> invalidDataPaths = FSUtils.getAllDataFilesForMarkers(fs, basePath, instantTs, markerDir.toString());
      List<String> validDataPaths = stats.stream().map(w -> String.format("%s/%s", basePath, w.getPath()))
              .filter(p -> p.endsWith(".parquet")).collect(Collectors.toList());
      // Contains list of partially created files. These needs to be cleaned up.
      invalidDataPaths.removeAll(validDataPaths);
      if (!invalidDataPaths.isEmpty()) {
        LOG.info(
                "Removing duplicate data files created due to spark retries before committing. Paths=" + invalidDataPaths);
      }

      Map<String, List<Pair<String, String>>> groupByPartition = invalidDataPaths.stream()
              .map(dp -> Pair.of(new Path(dp).getParent().toString(), dp)).collect(Collectors.groupingBy(Pair::getKey));

      if (!groupByPartition.isEmpty()) {
        // Ensure all files in delete list is actually present. This is mandatory for an eventually consistent FS.
        // Otherwise, we may miss deleting such files. If files are not found even after retries, fail the commit
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are present.
          waitForAllFiles(groupByPartition, ConsistencyGuard.FileVisibility.APPEAR);
        }

        // Now delete partially written files
        jsc.parallelize(new ArrayList<>(groupByPartition.values()), config.getFinalizeWriteParallelism())
                .map(partitionWithFileList -> {
                  final FileSystem fileSystem = metaClient.getFs();
                  LOG.info("Deleting invalid data files=" + partitionWithFileList);
                  if (partitionWithFileList.isEmpty()) {
                    return true;
                  }
                  // Delete
                  partitionWithFileList.stream().map(Pair::getValue).forEach(file -> {
                    try {
                      fileSystem.delete(new Path(file), false);
                    } catch (IOException e) {
                      throw new HoodieIOException(e.getMessage(), e);
                    }
                  });

                  return true;
                }).collect();

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(groupByPartition, ConsistencyGuard.FileVisibility.DISAPPEAR);
        }
      }
      // Now delete the marker directory
      deleteMarkerDir(instantTs);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }
}
