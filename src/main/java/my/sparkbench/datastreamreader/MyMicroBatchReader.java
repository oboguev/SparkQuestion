package my.sparkbench.datastreamreader;

import com.google.gson.Gson;

import my.sparkbench.event.Event;
import my.sparkbench.event.EventGenerator;
import my.sparkbench.util.Util;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MyMicroBatchReader implements MicroBatchReader
{
    private static final Logger logger = LoggerFactory.getLogger(MyMicroBatchReader.class);

    private int lastCommitted;
    private int start;
    private int end;
    private StructType schema;
    private final int batchSize = 100;

    public MyMicroBatchReader()
    {
        this.start = 0;
        this.end = 0;
        createSchema();
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end)
    {
        this.start = ((MyOffset) start.orElse(new MyOffset(0))).getOffset();
        this.end = ((MyOffset) end.orElse(new MyOffset(this.start + this.batchSize))).getOffset();
        if (this.end >= EventGenerator.EventCount)
            this.end = this.start;
        if (this.end != this.start)
            this.end = Math.min(this.end, EventGenerator.EventCount);
        logger.trace(String.format("setOffsetRange: %d ... %d", this.start, this.end));
    }

    @Override
    public Offset getStartOffset()
    {
        return new MyOffset(start);
    }

    @Override
    public Offset getEndOffset()
    {
        MyOffset offset = new MyOffset(end);
        return offset;
    }

    @Override
    public Offset deserializeOffset(String json)
    {
        Gson gson = new Gson();
        return gson.fromJson(json, MyOffset.class);
    }

    @Override
    public void commit(Offset end)
    {
        int offset = ((MyOffset) end).getOffset();
        this.lastCommitted = offset;
    }

    @Override
    public void stop()
    {
        Util.noop();
    }

    @Override
    public StructType readSchema()
    {
        return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions()
    {
        logger.info(String.format("planInputPartitions: %d ... %d", this.start, this.end));
        List<Object[]> rowChunk = new ArrayList<>();
        for (int offset = start; offset < end; offset++)
        {
            rowChunk.add(makeRow(offset));
        }

        return Collections.singletonList(new MyMicroBatchPartition(rowChunk));
    }

    private void createSchema()
    {
        this.schema = new StructType();
        this.schema = this.schema.add(new StructField("source", DataTypes.StringType, false, Metadata.empty()));
        this.schema = this.schema.add(new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty()));
        for (int k = 0; k < EventGenerator.NPROPS_LONG; k++)
            this.schema = this.schema.add(new StructField("I" + k, DataTypes.LongType, false, Metadata.empty()));
        for (int k = 0; k < EventGenerator.NPROPS_DOUBLE; k++)
            this.schema = this.schema.add(new StructField("D" + k, DataTypes.DoubleType, false, Metadata.empty()));
    }

    private Object[] makeRow(int offset)
    {
        long ts = EventGenerator.StartingTime + (offset / EventGenerator.NumberOfDevices) * EventGenerator.EventFrequency;
        int ndev = 1 + (offset % EventGenerator.NumberOfDevices);

        Object[] row = new Object[2 + EventGenerator.NPROPS_LONG + EventGenerator.NPROPS_DOUBLE];
        Event event = new Event(String.format("DEV-%04d", ndev), ts);

        int n = 0;
        row[n++] = UTF8String.fromString(event.source);
        row[n++] = new Long(event.time * 1000); // expects unix time in usec

        row[n++] = new Long(event.I0);
        row[n++] = new Long(event.I1);
        row[n++] = new Long(event.I2);
        row[n++] = new Long(event.I3);
        row[n++] = new Long(event.I4);
        row[n++] = new Long(event.I5);
        row[n++] = new Long(event.I6);
        row[n++] = new Long(event.I7);
        row[n++] = new Long(event.I8);
        row[n++] = new Long(event.I9);

        row[n++] = new Double(event.D0);
        row[n++] = new Double(event.D1);
        row[n++] = new Double(event.D2);
        row[n++] = new Double(event.D3);
        row[n++] = new Double(event.D4);
        row[n++] = new Double(event.D5);
        row[n++] = new Double(event.D6);
        row[n++] = new Double(event.D7);
        row[n++] = new Double(event.D8);
        row[n++] = new Double(event.D9);

        logger.trace(String.format("Emitting event for device %s, time %f sec. from start",
                                   event.source,
                                   (event.time - EventGenerator.StartingTime) / 1000.0));

        return row;
    }
}
