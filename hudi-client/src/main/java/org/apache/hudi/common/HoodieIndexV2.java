package org.apache.hudi.common;

public interface HoodieIndexV2<IN, OUT> {
    IN fetchRecordLocation(IN inputs);

    IN tagLocation(IN inputs, HoodieEngineContext context, HoodieTable table);

    OUT updateLocation(IN inputs);
}
