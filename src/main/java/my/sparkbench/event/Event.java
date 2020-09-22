package my.sparkbench.event;

import java.io.Serializable;

public class Event implements Serializable
{
    private static final long serialVersionUID = 1L;

    public Event(String source, long time)
    {
        this.source = new String(source);
        this.time = time;
    }

    @SuppressWarnings("unused")
    private Event()
    {
        // no default constructor
    }

    public String source;
    public long time;

    public long I0 = makeLong();
    public long I1 = makeLong();
    public long I2 = makeLong();
    public long I3 = makeLong();
    public long I4 = makeLong();
    public long I5 = makeLong();
    public long I6 = makeLong();
    public long I7 = makeLong();
    public long I8 = makeLong();
    public long I9 = makeLong();

    public double D0 = makeDouble();
    public double D1 = makeDouble();
    public double D2 = makeDouble();
    public double D3 = makeDouble();
    public double D4 = makeDouble();
    public double D5 = makeDouble();
    public double D6 = makeDouble();
    public double D7 = makeDouble();
    public double D8 = makeDouble();
    public double D9 = makeDouble();

    static private long makeLong()
    {
        return 10;
    }

    static private double makeDouble()
    {
        return 10.0;
    }
}
