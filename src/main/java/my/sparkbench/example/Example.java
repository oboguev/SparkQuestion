package my.sparkbench.example;

import java.io.File;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.slf4j.bridge.SLF4JBridgeHandler;

import my.sparkbench.datastreamreader.MyMicroBatchReader;
import my.sparkbench.event.EventGenerator;
import my.sparkbench.util.Util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

public class Example implements Serializable
{
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    private static final String DataDir = "/home/sergey/spark/data/device-events";

    private SparkSession sparkSession;
    private Dataset<Row> dataset;

    public static void main(String[] args)
    {
        new Example().do_main();
    }

    private void do_main()
    {
        try
        {
            logger.info("Starting...");
            exec_main();
            logger.info("Completed.");
        }
        catch (Exception ex)
        {
            logger.error("Exception", ex);
        }
    }

    private void exec_main() throws Exception
    {
        sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        sparkSession.conf().set("spark.sql.session.timeZone", "UTC");

        if (Util.True)
        {
            dataset = sparkSession
                                  .readStream()
                                  .format("my.sparkbench.datastreamreader.MyStreamingSource")
                                  .load();
        }
        else
        {
            EventGenerator.generateCsv(DataDir + File.separator + "device-events.csv");

            dataset = sparkSession
                                  .readStream()
                                  .option("header", "true")
                                  .option("timestampFormat", "yyyy:MM:dd HH:mm:ss.SSS")
                                  .schema(new MyMicroBatchReader().readSchema())
                                  .csv(DataDir);
        }

        // writeSourceAsStream();
        streamGroupByResult();
        // streamForeach();
    }

    private void writeSourceAsStream() throws Exception
    {
        StreamingQuery query = dataset.writeStream().format("console").option("truncate", "false").outputMode(OutputMode.Append()).start();
        query.awaitTermination();
    }

    private void streamGroupByResult() throws Exception
    {
        Dataset<Row> ds1 = dataset
                                  .withWatermark("timestamp", "1 second")
                                  .groupBy(
                                           functions.window(dataset.col("timestamp"), "1 second", "1 second"),
                                           dataset.col("source"))
                                  .agg(
                                       functions.avg("D0").as("AVG_D0"),
                                       functions.avg("I0").as("AVG_I0"))
                                  .orderBy("window");

        Dataset<Row> ds2 = dataset
                                  .groupBy(
                                           functions.window(dataset.col("timestamp"), "1 second", "1 second"),
                                           dataset.col("source"))
                                  .count()
                                  .orderBy("window");

        StreamingQuery query = ds1.writeStream()
                                  .outputMode("complete")
                                  .format("console")
                                  .option("truncate", "false")
                                  .option("numRows", Integer.MAX_VALUE)
                                  .start();

        query.awaitTermination();
    }

    private void streamForeach() throws Exception
    {
        Dataset<Row> ds1 = dataset
                                  .withWatermark("timestamp", "1 second")
                                  .groupBy(
                                           functions.window(dataset.col("timestamp"), "1 second", "1 second"),
                                           dataset.col("source"))
                                  .agg(
                                       functions.avg("D0").as("AVG_D0"),
                                       functions.avg("I0").as("AVG_I0"))
                                  .orderBy("window");

        Dataset<Row> ds2 = dataset
                                  .groupBy(
                                           functions.window(dataset.col("timestamp"), "1 second", "1 second"),
                                           dataset.col("source"))
                                  .count()
                                  .orderBy("window");

        StreamingQuery query = ds1
                                  .writeStream()
                                  // .outputMode("append")
                                  .outputMode("complete")
                                  .option("numRows", Integer.MAX_VALUE)
                                  .foreach(
                                           new ForeachWriter<Row>()
                                           {
                                               @Override
                                               public boolean open(long partitionId, long version)
                                               {
                                                   return true;
                                               }

                                               @Override
                                               public void process(Row record)
                                               {
                                                   logger.info("--- Row ----");
                                               }

                                               @Override
                                               public void close(Throwable errorOrNull)
                                               {
                                               }
                                           })
                                  .start();

        query.awaitTermination();
    }
}
