package my.sparkbench.datastreamreader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import java.util.List;

class MyMicroBatchPartition implements InputPartition<InternalRow>
{
    private static final long serialVersionUID = 1L;

    private List<Object[]> rowChunk;

    MyMicroBatchPartition(List<Object[]> rowChunk)
    {
        this.rowChunk = rowChunk;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader()
    {
        return new MyMicroBatchPartitionReader(rowChunk);
    }
}
