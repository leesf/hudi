package org.apache.hudi.common.rdd;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieIndexV2;
import org.apache.hudi.common.HoodieTable;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.spark.api.java.JavaRDD;

public class HoodieRDDBloomIndexV2<T extends HoodieRecordPayload> implements
    HoodieIndexV2<HoodieWriteInput<JavaRDD<HoodieRecord<T>>>, HoodieWriteOutput<JavaRDD<WriteStatus>>> {

    @Override
    public HoodieWriteInput<JavaRDD<HoodieRecord<T>>> fetchRecordLocation(
        HoodieWriteInput<JavaRDD<HoodieRecord<T>>> inputs) {
        return null;
    }

    @Override
    public HoodieWriteInput<JavaRDD<HoodieRecord<T>>> tagLocation(
        HoodieWriteInput<JavaRDD<HoodieRecord<T>>> inputs, HoodieEngineContext context,
        HoodieTable table) {
        return null;
    }

    @Override
    public HoodieWriteOutput<JavaRDD<WriteStatus>> updateLocation(
        HoodieWriteInput<JavaRDD<HoodieRecord<T>>> inputs) {
        return null;
    }
}
