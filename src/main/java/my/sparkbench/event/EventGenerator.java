package my.sparkbench.event;

import java.time.Instant;

import my.sparkbench.util.Util;

public class EventGenerator
{
    /*
     * Number of integer (long) properties in event
     */
    public static final int NPROPS_LONG = 10;

    /*
     * Number of double properties in event
     */
    public static final int NPROPS_DOUBLE = 10;

    /*
     * Number of devices
     */
    public static final int NumberOfDevices = 4;

    /*
     * Starting time (Unix ms)
     */
    public static final long StartingTime = 100 * 1000;

    /*
     * Event frequency (ms)
     */
    public static final long EventFrequency = 500;

    /*
     * Event count to generate
     */
    public static final int EventCount = NumberOfDevices * 1000;

    /*
     * Write events to CSV file
     */
    public static void generateCsv(String path) throws Exception
    {
        StringBuffer sb = new StringBuffer();
        String nl = "\n";
        sb.append("source,timestamp,I0,I1,I2,I3,I4,I5,I6,I7,I8,I9,D0,D1,D2,D3,D4,D5,D6,D7,D8,D9" + nl);

        for (int k = 0; k < EventCount; k++)
        {
            long ts = EventGenerator.StartingTime + (k / EventGenerator.NumberOfDevices) * EventGenerator.EventFrequency;
            int ndev = 1 + (k % EventGenerator.NumberOfDevices);

            Instant instant = Instant.ofEpochMilli(ts);
            String timestamp = instant.toString();
            timestamp = timestamp.replace("T", " ");
            if (timestamp.endsWith("Z"))
                timestamp = timestamp.substring(0, timestamp.length() - 1);
            if (!timestamp.contains("."))
                timestamp += ".000";

            Event event = new Event(String.format("DEV-%04d", ndev), ts);
            sb.append(String.format("%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f" + nl,
                                    event.source,
                                    timestamp,
                                    event.I0,
                                    event.I1,
                                    event.I2,
                                    event.I3,
                                    event.I4,
                                    event.I5,
                                    event.I6,
                                    event.I7,
                                    event.I8,
                                    event.I9,
                                    event.D0,
                                    event.D1,
                                    event.D2,
                                    event.D3,
                                    event.D4,
                                    event.D5,
                                    event.D6,
                                    event.D7,
                                    event.D8,
                                    event.D9));
        }

        Util.writeAsUTF8File(path, sb.toString());
    }
}
