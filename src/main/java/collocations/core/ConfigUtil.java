package collocations.core;

import org.apache.hadoop.conf.Configuration;

public final class ConfigUtil {
    private ConfigUtil() {}

    public static String sanitize(String key, String value) {
        if (value == null) {
            return null;
        }
        return value.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]", "");
    }

    public static void validateNoControlChars(String key, String value) {
        if (value == null) {
            return;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 0x00 && c <= 0x08) || (c >= 0x0B && c <= 0x0C) || (c >= 0x0E && c <= 0x1F) || c == 0x7F) {
                throw new RuntimeException("Illegal control character in " + key + " at index " + i + " code=" + Integer.toHexString(c));
            }
        }
    }

    public static void setCommonOutputs(Configuration conf) {
        conf.set("mapreduce.output.textoutputformat.separator", "\t");
        conf.setBoolean("mapreduce.job.user.classpath.first", true);
    }
}
