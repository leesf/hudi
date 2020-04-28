package org.apache.hudi.common.rdd;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.commit.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HoodieRDDCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieRDDTable<T> {

  public HoodieRDDCopyOnWriteTable(HoodieWriteConfig config,
                                   HoodieTableMetaClient metaClient) {
    this(new JavaSparkContext(), config, metaClient);
  }

  public HoodieRDDCopyOnWriteTable(JavaSparkContext jsc, HoodieWriteConfig config,
                                   HoodieTableMetaClient metaClient) {
    super(jsc, config, metaClient);
  }

  @Override
  public HoodieWriteMetadata upsert(String instantTime,
      HoodieWriteInput<JavaRDD<HoodieRecord<T>>> records) {
    return null;
  }

  @Override
  public HoodieWriteMetadata insert(String instantTime,
      HoodieWriteInput<JavaRDD<HoodieRecord<T>>> records) {
    return null;
  }

  @Override
  public HoodieWriteMetadata bulkInsert(String instantTime,
      HoodieWriteInput<JavaRDD<HoodieRecord<T>>> records,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteMetadata delete(String instantTime, HoodieWriteKey<JavaRDD<HoodieKey>> keys) {
    return null;
  }

  @Override
  public HoodieWriteMetadata upsertPrepped(String instantTime,
      HoodieWriteInput<JavaRDD<HoodieRecord<T>>> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata insertPrepped(String instantTime,
      HoodieWriteInput<JavaRDD<HoodieRecord<T>>> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata bulkInsertPrepped(String instantTime,
      HoodieWriteInput<JavaRDD<HoodieRecord<T>>> preppedRecords,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> compact(String compactionInstantTime,
      HoodieCompactionPlan compactionPlan) {
    return null;
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime) {
    return null;
  }

  @Override
  public HoodieRollbackMetadata rollback(String rollbackInstantTime, HoodieInstant commitInstant,
      boolean deleteInstants) {
    return null;
  }

  @Override
  public HoodieRestoreMetadata restore(String restoreInstantTime, String instantToRestore) {
    return null;
  }
}
