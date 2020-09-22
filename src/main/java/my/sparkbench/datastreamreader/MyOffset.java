package my.sparkbench.datastreamreader;

import com.google.gson.Gson;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

public class MyOffset extends Offset
{
    private int offset;

    public MyOffset(int offset)
    {
        this.offset = offset;
    }

    @Override
    public String json()
    {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public void setOffset(int offset)
    {
        this.offset = offset;
    }

    public int getOffset()
    {
        return offset;
    }
}
