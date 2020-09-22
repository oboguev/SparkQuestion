package my.sparkbench.datastreamreader;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import scala.collection.JavaConversions;

public class MyMicroBatchPartitionReader implements InputPartitionReader<InternalRow>
{
    private List<Object[]> rowChunk;
    private int counter;

    MyMicroBatchPartitionReader(List<Object[]> rowChunk)
    {
        this.rowChunk = rowChunk;
        this.counter = 0;
    }

    @Override
    public boolean next()
    {
        return counter < rowChunk.size();
    }

    @Override
    public InternalRow get()
    {
        Object[] row = rowChunk.get(counter++);
        return InternalRow.apply(JavaConversions.asScalaBuffer(Arrays.asList(row)).seq());
    }

    @Override
    public void close()
    {
    }
}
