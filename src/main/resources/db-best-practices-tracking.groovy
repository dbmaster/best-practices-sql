import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry

import com.branegy.service.base.api.ProjectService
import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcConnector
import com.branegy.email.EmailSender

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.text.SimpleDateFormat;
import io.dbmaster.tools.excelsync.ExcelSync;
import org.dbmaster.dbstyle.api.InputFilter;

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

def cl = this.getClass().getClassLoader();
InputFilter filter = cl.parseClass(p_filter).newInstance();

filter.init([dbm:dbm,logger:logger]);


ExcelSync excelSync = new ExcelSync(
    ["Server","Environment","CheckID","Object Type","Object Name","Severity","Issue","TechKey","Status","Change Date"],   
    ["Server","CheckID","Object Type","Object Name","TechKey"],
    "Status",
    "Change Date",
    "New", 
    "AutoClosed", 
    ["Ignore"] as Set,
    ["Ignore", "AutoClosed", "Closed"] as Set,
    new File(p_tracking_file),
    logger,
    p_backup
);

dbConnections.each { connectionInfo ->
try {
    connector = ConnectionProvider.getConnector(connectionInfo)
    def serverName = connectionInfo.getName()
    if (!(connector instanceof JdbcConnector)) {
        logger.info("Skipping checks for connection ${connectionInfo.getName()} as it is not a database one")
        return
    }
    if (!filter.accept(connectionInfo)){
        return;
    }
    
    connection = connector.getJdbcConnection(null)
    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
    dbm.closeResourceOnExit(connection)
    
    def dialect = filter.filter(connectionInfo,connector.connect(), connection);

    modules.each { module ->
        logger.info ("Running check ${module} for ${connectionInfo.getName()} ")

        try {
            //def checkClass = new GroovyClassLoader().loadClass("org.dbmaster.dbstyle.checks.${module}")
            
            def sourceCode = new GroovyCodeSource(cl.getResource("org/dbmaster/dbstyle/checks/${module}.groovy"))
            def checkClass = new GroovyClassLoader(cl).parseClass(sourceCode)
            
            def check = checkClass.newInstance()
            check.init()
            check.logger = logger

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
                def environment = connectionInfo.getCustomData("Environment")
                
                excelSync.addRow(connectionInfo.getName(), environment ?: "", module, issue.object_type, issue.object_name,
                    issue.severity, issue.description, issue.object_key == null ? "" :  serverName + "."+issue.object_key );
                
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

filter.destroy();

int[] score = excelSync.syncAndReturnScore();

println "During update ${score[0]} new issue(s) were found and ${score[1]} were automatically closed. Total open issues ${score[2]}<br/>"+
 "Review issues at <a href='${new File(p_tracking_file).toURI()}'>${p_tracking_file}</a>";
if (p_email != null){
    EmailSender sender = dbm.getService(EmailSender.class);
    String msg = 
"""
Best practices tracking worksheet was automatically updated by DBMaster.

During update ${score[0]} new issue(s) were found and ${score[1]} were automatically closed.

Total open issues ${score[2]}

Review issues at ${p_tracking_file}.

---------------------------------------
This message was automatically generated by DBMaster plugin
""";
    
    sender.createMessage(p_email, "SQL Server best practices: worksheet updated", msg, false);
    sender.sendMessage();
}

logger.info("Check is completed")