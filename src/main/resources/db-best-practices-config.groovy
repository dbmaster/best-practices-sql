import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map.Entry
import com.branegy.service.base.api.ProjectService
import io.dbmaster.dbstyle.api.InputFilter
import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcConnector
//import com.branegy.dbmaster.custom.field.server.api.ICustomFieldService
//import com.branegy.dbmaster.custom.*
// import io.dbmaster.tools.datatracker.DBMasterSync
// import com.branegy.dbmaster.custom.CustomFieldConfig.Type


def toURL = { link ->
    link==null ? "NULL" : link.encodeURL().replaceAll("\\+", "%20")
}
String.metaClass.encodeURL = { java.net.URLEncoder.encode(delegate) }

String projectName =  dbm.getService(ProjectService.class).getCurrentProject().getName();

def acceptable_severity = {input_severity ->
    def acceptable_levels = []

    switch(input_severity) {
        case "":
        case "info":
        case null:
            acceptable_levels.addAll(['error', 'warning', 'info']);
            break;
        case "warning":
            acceptable_levels.addAll(['error', 'warning'])
            break;
        case "error":
            acceptable_levels.addAll(['error']);
            break;
    }
    return acceptable_levels;
}

def linkToObject = { type, serverName, objectName  ->
    prefix = "#inventory/project:${toURL(projectName)}"
    if (type.equals("Application")) {
        return "${prefix}/applications/application:${toURL(objectName)}"
    } else if (type.equals("Server")) {
        return  "${prefix}/servers/server:${toURL(serverName)}"
    } else if (type.equals("Database")) {
        return  "${prefix}/databases/connection:${toURL(serverName)},db:${toURL(objectName)}"
    } else if (type.equals("Connection")) {
        return  "${prefix}/connections/connection:${toURL(serverName)}" 
    } else if (type.equals("Job")) {
        // TODO we do not have any information about jobs in dbmaster yet
        return  "${prefix}/databases" 
    } else {
        throw new RuntimeException("Object type ${type} was not expected")
    }
}

def required_severity_levels = acceptable_severity(p_severity)

connectionSrv = dbm.getService(ConnectionService.class)
//<DatabaseConnection> getConnectionSlice(QueryRequest params, String name);
    
    
    
// def dbConnections
// if (p_servers!=null && p_servers.size()>0) {
//    dbConnections = p_servers.collect { serverName -> connectionSrv.findByName(serverName) }
//} else {
//    dbConnections  = connectionSrv.getConnectionList()
//}

def config = new XmlSlurper().parseText(p_config)
def scopes = [:]
config.scope.each { scope ->
    logger.debug ( "Scope:"+scope.@name )
    def name = scope.@name.toString()
    scopes[name] = [:]
    scope.children().each { child ->
        scopes[name].put(child.name(), ["include" : child.@include, "exclude": child.@exclude] )
        logger.debug("Add filter "+child.name() + " include:"  + child.@include + " exclude:" + child.@exclude)
    }
}
        
println  """
   <table class="simple-table" cellspacing="0" cellpadding="10">
        <tr style="background-color:#EEE">
            <td>Server</td>
            <td>Environment</td>
            <td>CheckID</td>
            <td>Object Type</td>
            <td>Object Name</td>
            <td>Severity</td>
            <td>Issue</td>
            <td>TechKey</td>
        </tr>
"""

def cl = this.getClass().getClassLoader()

config.checkSet.each { checkSet ->
    logger.info( "starting checkSet:"+checkSet.@scope)
    
    def scope = checkSet.@scope
    if (scope==null) {
        logger.error("No scope defined")
        return
    }

    def dbConnections = []
    scope.toString().split(",").each { it ->        
        if (it.equals("all")) {
            dbConnections  = connectionSrv.getConnectionList()
            return
        } else {        
            def scopeDef = scopes[it.trim()]
            if (scopeDef == null) {
                logger.error("Scope ${it} not defined")
            } else {
                if (scopeDef.servers!=null  && scopeDef.servers.include!=null) {
                    def query = new QueryRequest(scopeDef.servers.include.toString())
                    dbConnections.addAll( connectionSrv.getConnectionSlice(query, null) )
                } else {
                    // no servers scope defined. using all by default
                    logger.debug("Server scope not defined. Using all servers")
                    dbConnections  = connectionSrv.getConnectionList()
                    return
                }
            }
        }
    }

    // 
    def checks = [:]

    dbConnections.each { connectionInfo ->
        try {
            connector = ConnectionProvider.getConnector(connectionInfo)
            def serverName = connectionInfo.getName()
            if (!(connector instanceof JdbcConnector)) {
                logger.info("Skipping checks for connection ${connectionInfo.getName()} as it is not a database one")
                return
            }

            connection = connector.getJdbcConnection(null)
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
            dbm.closeResourceOnExit(connection)

            def dialect = connector.connect(); //filter==null ? 
                                       //: filter.filter(connectionInfo,connector.connect(), connection)
        
            checkSet.check.each { checkXML ->
                def checkName = checkXML.@name
                if (checkName==null) {
                    logger.error("Check name is not defined")
                    return
                }
                
                logger.info ("Running check ${checkName} for ${serverName} ")
                
                // dbmSync.loadRecords("Server=\"${serverName}\" && CheckID=\"${checkName}\"")

                try {
                    // TODO check if source does not exist
                    def check = checks[checkName]
                    if (check==null) {
                        def moduleImpl = cl.getResource("io/dbmaster/dbstyle/checks/${checkName}.groovy")
                        if (moduleImpl == null) { 
                           throw new Exception("Implementation for check ${checkName} was not found")
                        }
                        def sourceCode = new GroovyCodeSource(moduleImpl)
                        def checkClass = new GroovyClassLoader(cl).parseClass(sourceCode)
                        
                        check = checkClass.newInstance()
                        check.logger = logger
                        check.dbm = dbm
                        check.init()
                        checks[checkName] = check
                    }
                   
                    def collector = new io.dbmaster.dbstyle.api.MessageCollector()
                    check.setMessageCollector(collector)

                    def parameters = [:]
                    

                    checkXML.property.each { p ->
                        logger.debug ( "property:"+p.@name+":"+p.@value )
                        def key = p.@name.toString()
                        def value = p.@value.toString()
                        def currentValue = parameters[key]
                        if (currentValue!=null) {
                            if (currentValue instanceof List) {
                                currentValue.add(value)
                            } else {
                                parameters[key] = [currentValue, value]
                            }
                        } else {
                            parameters[key] = value
                        }
                    }

                    /*
                    if (p_properties!=null) {
                        p_properties.replaceAll("\r\n", "\n").split("\n").each {
                            def key_value = it.split("=")
                            if (key_value.length>=2) {
                                def value = key_value[1..key_value.length-1].join("=")
                                def currentValue = parameters[key_value[0]]
                                if (currentValue!=null) {
                                    if (currentValue instanceof List) {
                                        currentValue.add(value)
                                    } else {
                                        parameters[key_value[0]] = [currentValue, value]
                                    }
                                } else {
                                    parameters[key_value[0]] = value
                                }
                                
                                logger.info("Setting property ${key_value[0]} to ${value}")
                                
                            } else {
                                logger.warn("Cannot recognize property ${it}")
                            }
                        }
                    } 
                    */
                    
                    check.setParameters(parameters)
                    check.check(connection, dialect)

                    // -----------------Layout ------------------------------------------------------------------
                    collector.messages.sort { it -> it.object_type + it.object_name }
                    
                    
                    collector.messages.each { issue ->
                        if (!required_severity_levels.contains(issue.severity)) {
                            return;
                        }
                        def environment = connectionInfo.getCustomData("Environment")
                        
                        
                    println """
                       <tr>
                         <td>${connectionInfo.getName()}</td>
                         <td>${environment ?: ""}</td>
                         <td>${checkName}</td>
                         <td>${issue.object_type}</td>
                         <td><a href="${linkToObject(issue.object_type, serverName, issue.object_name)}">${issue.object_name}</a></td>
                         <td>${issue.severity}</td>
                         <td>${issue.description}</td>
                         <td>${issue.object_key == null ? "" :  serverName + "."+issue.object_key}</td></tr>"""
                    }
                    // def statistics = dbmSync.completeSync()
                } catch (Exception e) {
                    def msg = "Cannot check ${checkName} for ${serverName}: ${e.getMessage()}"
                    org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg, e)
                    logger.error(msg)
                }
            } // end of checks loop
        } catch (Exception e) {
            def msg = "Error occurred "+e.getMessage()
            org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
            logger.error (msg)
        }
    }
}
println "</table>"

logger.info("Check completed")