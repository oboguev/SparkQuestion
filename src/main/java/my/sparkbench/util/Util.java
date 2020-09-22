package my.sparkbench.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util
{
    private final static Logger logger = LoggerFactory.getLogger(Util.class);

    public static boolean True = true;
    public static boolean False = false;

    public static void noop()
    {
    }

    public static void release(AutoCloseable c)
    {
        if (c != null)
        {
            try
            {
                c.close();
            }
            catch (Exception ex)
            {
                logger.error("Failed to close " + c.getClass().getName(), ex);
            }
        }
    }

    public static void join(Thread t)
    {
        for (;;)
        {
            try
            {
                switch (t.getState())
                {
                case TERMINATED:
                case NEW:
                    return;

                default:
                    break;
                }

                t.join();
                return;
            }
            catch (InterruptedException ex)
            {
                Util.noop();
            }
        }
    }

    static public void writeAsFile(String path, byte[] bytes) throws Exception
    {
        Files.write(Paths.get(path), bytes);
    }

    static public void writeAsUTF8File(String path, String data) throws Exception
    {
        writeAsFile(path, data.getBytes(StandardCharsets.UTF_8));
    }
}
