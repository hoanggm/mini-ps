package org.mini.pubsub.util;

import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

public class CommonUtil {

    private CommonUtil() {
    }

    public static String formatDate(Temporal date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return formatter.format(date);
    }
}
