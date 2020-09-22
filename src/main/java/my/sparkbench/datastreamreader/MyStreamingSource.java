package my.sparkbench.datastreamreader;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import my.sparkbench.util.Util;

import java.util.Optional;

/**
 * This class is custom implementation of streaming interfaces for Apache Spark
 */
public class MyStreamingSource implements DataSourceV2, MicroBatchReadSupport // , ContinuousReadSupport
{
    public MyStreamingSource()
    {
        Util.noop();
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options)
    {
        // return new MyMicroBatchReader(options.get("filepath").get());
        return new MyMicroBatchReader();
    }
}
