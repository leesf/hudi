package org.apache.hudi.common;

public class HoodieWriteInput<IN> {

    private IN inputs;

    public HoodieWriteInput(IN inputs) {
        this.inputs = inputs;
    }

    public IN getInputs() {
        return inputs;
    }
}
