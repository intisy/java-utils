package io.github.intisy.utils.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

public class Log {
    private static final Logger logger = LoggerFactory.getLogger("io.github.intisy.utils");

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

    public static void error(Marker marker, String format, Object... argArray) {
        logger.error(marker, format, argArray);
    }

    public static void fatal(String message) {
        logger.error(message);
    }

    public static void fatal(String message, Throwable t) {
        logger.error(message, t);
    }

    public static void setLevel(String loggerName, String level) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger logbackLogger = loggerContext.getLogger(loggerName);
        logbackLogger.setLevel(Level.toLevel(level));
        Log.info("Log level for '" + loggerName + "' set to " + level);
    }

    public static void setRootLevel(String level) {
        setLevel(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME, level);
    }
}
