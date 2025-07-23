package io.github.intisy.utils.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Log {
    private static final Logger logger = LogManager.getLogger(Log.class);

    public static void trace(String message) {
        logger.trace(message);
    }

    public static void trace(String message, Throwable t) {
        logger.trace(message, t);
    }

    public static void debug(String message) {
        logger.debug(message);
    }

    public static void debug(String message, Throwable t) {
        logger.debug(message, t);
    }

    public static void info(String message) {
        logger.info(message);
    }

    public static void info(String message, Throwable t) {
        logger.info(message, t);
    }

    public static void warn(String message) {
        logger.warn(message);
    }

    public static void warn(String message, Throwable t) {
        logger.warn(message, t);
    }

    public static void error(String message) {
        logger.error(message);
    }

    public static void error(String message, Throwable t) {
        logger.error(message, t);
    }

    public static void fatal(String message) {
        logger.fatal(message);
    }

    public static void fatal(String message, Throwable t) {
        logger.fatal(message, t);
    }
}
