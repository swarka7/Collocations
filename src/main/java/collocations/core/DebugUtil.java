package collocations.core;

import java.io.PrintWriter;
import java.io.StringWriter;

public final class DebugUtil {
    private DebugUtil() {}

    public static String stackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
