package io.dbmaster.dbstyle.api

import java.sql.Connection
import com.branegy.scripting.DbMaster
import com.branegy.dbmaster.connection.Dialect

public abstract class Check {
    /**
     * the severity level of any violations found
     */
    private SeverityLevel severityLevel = SeverityLevel.ERROR

    /** the identifier of this check */
    private String id
    
    def logger

    /** the object for collecting messages. */
    private MessageCollector collector

    private Map parameters
    
    protected Context context

    public abstract void check(Connection connection, Dialect dialect)

    /**
     * Initialise the check. This is the time to verify that the check has
     * everything required to perform it job.
     */
    public void init(Context context) {
        this.context = context
        this.logger = context.logger
    }

    public void addMessage(Map messageProperties) {
        def key = messageProperties.object_key
        if (key==null) {
            context.logger.warn("Object key is not defined for check ${id}") 
        } else {
            key = context.serverName+"."+key
            for (Suppression suppression: context.suppressions) {
                if (suppression.check.equals(id) && suppression.checkKey( key )) {
                    context.logger.debug("Message suppressed: check ${id} key=${key}")
                    return
                }
            }
        }
        collector.add(new Message(messageProperties))
    }

    public void setParameters(Map parameters) {
       this.parameters = parameters
    }

    public Object getParameter(String parameterName) {
        return parameters[parameterName]
    }

    public void setMessageCollector(MessageCollector collector) {
       this.collector = collector
    }
}