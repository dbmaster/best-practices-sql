package io.dbmaster.dbstyle.api;

import java.sql.Connection;
import java.util.Map;

public class Check {
    /**
     * the severity level of any violations found
     */
    private SeverityLevel severityLevel = SeverityLevel.ERROR;

    /** the identifier of the reporter */
    private String id;

    /** the object for collecting messages. */
    private MessageCollector collector;

    private Map parameters;

    def logger

    public void check(Connection connection) {}

    /**
     * Initialise the check. This is the time to verify that the check has
     * everything required to perform it job.
     */
    public void init() {
    }

    public void addMessage(Map messageProperties) {
        collector.add(new Message(messageProperties))
    }

    public void setParameters(Map parameters) {
       this.parameters = parameters;
    }

    public Object getParameter(String parameterName) {
        return parameters[parameterName]
    }

    public void setMessageCollector(MessageCollector collector) {
       this.collector = collector;
    }
}
