package io.dbmaster.dbstyle.api

public enum SeverityLevel {
    IGNORE, INFO, WARNING,  ERROR;

    @Override
    public String toString() {
        return getName()
    }

    /**
     * @return the name of this severity level.
     */
    public String getName() {
        return name().toLowerCase()
    }

    public static SeverityLevel getInstance(String severity) {
        return valueOf(SeverityLevel.class, severity.trim().toUpperCase())
    }
}
