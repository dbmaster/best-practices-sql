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
import com.branegy.dbmaster.custom.field.server.api.ICustomFieldService
import com.branegy.dbmaster.custom.*
import io.dbmaster.tools.datatracker.DBMasterSync
import com.branegy.dbmaster.custom.CustomFieldConfig.Type


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
        [
          "FragmentedIndexes","UserObjectSystemDB"
        ]
}

    def trackingStorageType = "best practices"

    def createField = { fieldName, fieldType, required, readonly, keyfield = false ->
        CustomFieldConfig field = new CustomFieldConfig()
        field.setName(fieldName)
        field.setType(fieldType)
        field.setRequired(required)
        field.setReadonly(readonly)
        field.setKey(keyfield)
        return field
    }
    
    def fields = [
        createField("Record key",Type.STRING,true,true,true),
        createField("Server",Type.STRING,true,true),
        createField("Environment",Type.STRING,true,true),
        createField("CheckID",Type.STRING,true,true),
        createField("Object Type",Type.STRING,true,true),
        createField("Object Name",Type.STRING,true,true),
        createField("Object Key",Type.STRING,true,true),
        createField("Severity",Type.STRING,true,true),
        createField("Issue",Type.STRING,true,true),
        createField("Review status",Type.STRING,false,false),
        // TODO Add values: New, Assigned, Ignore, AutoClosed, Closed
        createField("Owner",Type.STRING,false,false),
        createField("Notes",Type.TEXT,false,false)
    ]
    
    DBMasterSync dbmSync = new DBMasterSync(
        fields,    
        ["Server", "CheckID", "Object Type", "Object Name", "Object Key"], // key fields
        ["Record key", "Review status", "Owner", "Notes"] as Set, // do not update columns
        "Review status",
        "New", 
        "AutoClosed", 
        ["Ignore"] as Set, // ignoreAutoCloseStatusSet
        ["Ignore", "AutoClosed", "Closed"] as Set, // nonOpenStatusSet
        trackingStorageType,
        dbm,
        logger    
    )

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
            <td>Status</td>
            <td>Owner</td>
            <td>Notes</td>
            <td>TechKey</td>
        </tr>
"""

InputFilter filter = null
def cl = this.getClass().getClassLoader()

if (p_filter!=null) {
    filter = cl.parseClass(p_filter).newInstance()
    filter.init([dbm:dbm, logger:logger])
}

dbConnections.each { connectionInfo ->
    try {
        connector = ConnectionProvider.getConnector(connectionInfo)
        def serverName = connectionInfo.getName()
        if (!(connector instanceof JdbcConnector)) {
            logger.info("Skipping checks for connection ${connectionInfo.getName()} as it is not a database one")
            return
        }
        if (filter!=null && !filter.accept(connectionInfo)){
            return;
        }

        connection = connector.getJdbcConnection(null)
        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
        dbm.closeResourceOnExit(connection)

        def dialect = filter==null ? connector.connect()
                                   : filter.filter(connectionInfo,connector.connect(), connection)
    
        modules.each { module ->
            logger.info ("Running check ${module} for ${serverName} ")
            
            dbmSync.loadRecords("Server=\"${serverName}\" && CheckID=\"${module}\"")

            try {
                // TODO check if source does not exist
                def moduleImpl = cl.getResource("io/dbmaster/dbstyle/checks/${module}.groovy")
                if (moduleImpl == null) { 
                   throw new Exception("Implementation for check ${module} does not exist")
                }
                def sourceCode = new GroovyCodeSource(moduleImpl)
                def checkClass = new GroovyClassLoader(cl).parseClass(sourceCode)
                
                def check = checkClass.newInstance()
                check.init()
                check.logger = logger
               
                def collector = new io.dbmaster.dbstyle.api.MessageCollector()
                check.setMessageCollector(collector)
                
                def parameters = [:]
                parameters["dbmaster"] = dbm
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
                
                check.setParameters(parameters)
                check.check(connection, dialect)

                // -----------------Layout ------------------------------------------------------------------
                collector.messages.sort { it -> it.object_type + it.object_name }
                
                CustomObjectEntity record
                
                collector.messages.each { issue ->
                    if (!required_severity_levels.contains(issue.severity)) {
                        return;
                    }
                    def environment = connectionInfo.getCustomData("Environment")

                    record = dbmSync.checkRecord(
                        UUID.randomUUID().toString(),
                        serverName,
                        environment ?: "", 
                        module, 
                        issue.object_type, 
                        issue.object_name,
                        issue.object_key,
                        issue.severity, 
                        issue.description,
                        null,
                        null,
                        null
                     )
                    
                    println """
                   <tr>
                     <td>${connectionInfo.getName()}</td>
                     <td>${environment ?: ""}</td>
                     <td>${module}</td>
                     <td>${issue.object_type}</td>
                     <td><a href="${linkToObject(issue.object_type, serverName, issue.object_name)}">${issue.object_name}</a></td>
                     <td>${issue.severity}</td>
                     <td>${issue.description}</td>
                     <td>${record.getCustomData("Review status")?:""}</td>
                     <td>${record.getCustomData("Owner")?:""}</td>
                     <td>${record.getCustomData("Notes")?:""}</td>
                     <td>${issue.object_key == null ? "" :  serverName + "."+issue.object_key}</td></tr>"""
                }
                def statistics = dbmSync.completeSync()
            } catch (Exception e) {
                def msg = "Cannot check ${module} for ${serverName}: ${e.getMessage()}"
                org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg, e)
                logger.error(msg)
            }
        } // end of modules loop
    } catch (Exception e) {
        def msg = "Error occurred "+e.getMessage()
        org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
        logger.error (msg)
    }
}

println "</table>"

if (filter!=null) {
    filter.destroy();
}
logger.info("Check completed")
