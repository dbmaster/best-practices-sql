import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map.Entry
import com.branegy.service.base.api.ProjectService;
import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcConnector

def toURL = { link ->
    link==null ? "NULL" : link.encodeURL().replaceAll("\\+", "%20")
}
String.metaClass.encodeURL = { java.net.URLEncoder.encode(delegate) }

String projectName =  dbm.getService(ProjectService.class).getCurrentProject().getName();

def acceptable_severity = {input_severity ->
    def acceptable_levels = []

    switch(input_severity){
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
        return  "${prefix}/databases/server:${toURL(serverName)},db:${toURL(objectName)}"
    } else if (type.equals("Connection")) {
        // TODO here there should be a link to server
        return  "${prefix}/databases/search:ServerName=${toURL(serverName)}" 
    } else if (type.equals("Job")) {
        // TODO we do not have any information about jobs in dbmaster yet
        return  "${prefix}/databases" 
    } else {
        throw new RuntimeException("Object type ${type} was not expected")
    }
}

def required_severity_levels = acceptable_severity(p_severity)

// -----------------Rules---------------------------------------------------------------------------
connectionSrv = dbm.getService(ConnectionService.class)

def dbConnections
if (p_servers!=null && p_servers.size()>0) {
    dbConnections = p_servers.collect { serverName -> connectionSrv.findByName(serverName) }
} else {
    dbConnections  = connectionSrv.getConnectionList()
}
def modules
if (p_modules!=null && p_modules.size()>0) {
    modules = p_modules 
} else {
    modules  =
        ["NonDefaultStartupProc", "ForeignKeysNoIndexes",  "WideIndex",  "DuplicateIndexes",
         "ExtraIndexes", "FragmentedIndexes", "NoIndexes", "NoClusteredIndexes", "OutdatedStatistics",
         "AutoCreateStatistics",  "AutoUpdateStatistics", "TempDbFileSizeDifferent", "TempDbSingleFile",
         "UserAndTempDbSameDisk", "SmallTempDbSize", "AutoShrinkEnabled",  "LargeAutoGrowth",
         "ChecksumDisabled", "LowCompatibilityLevel", "LoginsWithoutPolicy",
         "SimpleRecoveryMode", "NonMasterCollation", "ServerAdmins", "DatabaseAdmins",  "JobOwnerIsAdmin",
         "AutoGrowthIsPercentage", "RedundantIndexes", "NoDataPurityCheck", "NoDbccCheck","OrphanedLogins",
         "OrphanedUsers"]
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

    modules.each { module ->
        logger.info ("Running check ${module} for ${connectionInfo.getName()} ")

        try {
            //def checkClass = new GroovyClassLoader().loadClass("org.dbmaster.dbstyle.checks.${module}")
            
            ClassLoader cl = this.getClass().getClassLoader();
            def sourceCode = new GroovyCodeSource(cl.getResource("org/dbmaster/dbstyle/checks/${module}.groovy"))
            def checkClass = new GroovyClassLoader(cl).parseClass(sourceCode)
            
            def check = checkClass.newInstance()
            check.init()
            check.logger = logger
            def dialect = connector.connect()
            def collector = new org.dbmaster.dbstyle.api.MessageCollector()
            check.setMessageCollector(collector)
            
            def parameters = [:]
            parameters["dbmaster"] = dbm
            if (p_properties!=null) {
                p_properties.replaceAll("\r\n", "\n").split("\n").each {
                    def key_value = it.split("=")
                    if (key_value.length>=2) {
                        def value = key_value[1..key_value.length-1].join("=")
                        parameters[key_value[0]] = value
                        logger.info("Setting property ${key_value[0]} to ${value}")
                    } else {                        System.out.println("Setting property ${key_value[0]} to '${value}'")

                        logger.error("Cannot recognize property ${it}")
                    }
                }
            }
            
            check.setParameters(parameters)
            check.check(connection, dialect)

            // -----------------Layout ------------------------------------------------------------------

            collector.messages.sort { it -> it.object_type + it.object_name }

            collector.messages.each { issue ->

               if (!required_severity_levels.contains(issue.severity)){
                   return;
               }
                def environment = connectionInfo.getProperties().find{it.key=="environment"}?.value
                println """
               <tr>
                 <td>${connectionInfo.getName()}</td>
                 <td>${environment ?: ""}</td>
                 <td>${module}</td>
                 <td>${issue.object_type}</td>

                 <td><a href="${linkToObject(issue.object_type, serverName, issue.object_name)}">${issue.object_name}</a></td>
                 <td>${issue.severity}</td>
                 <td>${issue.description}</td>
                 <td>${issue.object_key == null ? "" :  serverName + "."+issue.object_key}</td></tr>"""
            }
        } catch (Exception e) {
            def msg = "Cannot check ${module} for ${serverName} "+e.getMessage()
            org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
            logger.error (msg)
        }
    }
} catch (Exception e) {
    def msg = "Error occurred "+e.getMessage()
    org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
    logger.error (msg)
}

}
println "</table>"

logger.info("Check is completed")
