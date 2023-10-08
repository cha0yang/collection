package m;

import org.apache.flink.api.common.functions.Partitioner;

public class SinkScheduler implements Partitioner<Integer> {
    private final int batchSize;
    private int batchCount;
    private int index;

    public SinkScheduler(int batchSize) {
        this.batchSize = batchSize;
        this.batchCount = 0;
        this.index = 0;
    }

    @Override
    public int partition(Integer key, int numPartitions) {
        if (++this.batchCount > this.batchSize) {
            this.batchCount = 1;

            if (++this.index >= numPartitions) this.index = 0;
        }

        return this.index;
    }
}
