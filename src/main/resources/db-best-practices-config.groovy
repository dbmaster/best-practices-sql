import io.dbmaster.dbstyle.api.ObjectFilter
import io.dbmaster.dbstyle.api.Context
import io.dbmaster.dbstyle.api.Scope
import io.dbmaster.dbstyle.api.Suppression
import io.dbmaster.dbstyle.filter.InventoryFilter

import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.Dialect
import com.branegy.dbmaster.connection.JdbcDialect

import io.dbmaster.tools.DbmTools

def acceptable_severity = {input_severity ->
    def acceptable_levels = []
    switch(input_severity) {
        case "":
        case "info":
        case null:
            acceptable_levels.addAll(['error', 'warning', 'info'])
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

def getClassInstance = { sourceFile ->
    def cl = this.getClass().getClassLoader()
    def moduleImpl = cl.getResource(sourceFile)
    if (moduleImpl == null) { 
        return null
    } else {
        def sourceCode = new GroovyCodeSource(moduleImpl)
        def checkClass = new GroovyClassLoader(cl).parseClass(sourceCode)
        return checkClass.newInstance()
    }
}

def required_severity_levels = acceptable_severity(p_severity)
connectionSrv = dbm.getService(ConnectionService.class)

def tools = new DbmTools ( dbm, logger, getBinding().out)

def config = new XmlSlurper().parseText(p_config)
def issueIndex = 0
// INITIALIZE SCOPES AND FILTERS
def scopes = [:]
config.scope.each { scopeNode ->
    logger.debug ( "Scope:"+scopeNode.@name )
    Scope scope = new Scope(scopeNode.@name.toString())
    
    // def name =     
    scopes[scope.name] = scope
    scopeNode.children().each { childNode ->
        def childName = childNode.name()
        def filterParameters = [:]
        if (childName.equals("filter")) {
            String sourceCode = childNode.@class.toString().replaceAll("\\.","/")+".groovy"
            logger.debug("Loading filter ${sourceCode}")
            ObjectFilter filter = getClassInstance(sourceCode)
            childNode.property.each { p ->
                logger.debug ("Filter property key="+p.@name+" value="+p.@value )
                filterParameters[p.@name.toString()] = p.@value.toString()
            }
            filter.init(tools, filterParameters)
            scope.addFilter(filter)
        } else {
            logger.debug("Add filter "+childNode.name() + " i:"  + childNode.@include + " e:" + childNode.@exclude)            
            if (childNode.@include!=null) {
                def filter = new InventoryFilter(childName, childNode.@include.toString(), InventoryFilter.INCLUDE)
                filter.init(tools, filterParameters)
                scope.addFilter(filter)                
            }
            if (childNode.@exclude!=null) {
                def filter = new InventoryFilter(childName, childNode.@exclude.toString(), InventoryFilter.EXCLUDE)
                filter.init(tools, filterParameters)
                scope.addFilter(filter)
            }
        }
    }
}
        
println  """
   <table class="simple-table" cellspacing="0" cellpadding="10">
        <tr style="background-color:#EEE">
            <td>#</td>
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

Context context = new Context()
context.logger = logger
context.dbm = dbm
context.suppressions = config.suppressions.suppression.collect { 
                         new Suppression(it.@check.toString(), it.@key.toString()) 
                       }

def checks = [:]

config.checkSet.each { checkSet ->
    logger.info( "Starting checkSet with scope "+checkSet.@scope)
    
    def scopeAttr = checkSet.@scope
    context.scopeList = []
    if (scopeAttr==null) {
        logger.warn("No scope defined for checkset. Including everything")
    } else {
        for (String scopeId: scopeAttr.toString().split(",")) {
            scopeId = scopeId.trim()
            if (scopeId.equals("all")) {
                logger.info("Including everything into checkset scope")
                // empty scope means include everything
                context.scopeList.clear()
                break
            } else {        
                def scopeDef = scopes[scopeId]
                if (scopeDef == null) {
                    logger.error("Scope with name ${it} was not defined")
                } else {
                    logger.info("Including ${scopeId} into checkset")
                    context.scopeList.add ( scopeDef )
                }
            }
        }
    }


    connectionSrv.connectionList.each { connectionInfo ->
        try {
            context.serverName = connectionInfo.getName()            
            if (!context.isObjectInScope("DatabaseConnection", context.serverName)) {
                logger.debug("Skipping checks for connection ${context.serverName}. Out of scope")
                return
            }
            def dialect = ConnectionProvider.get().getDialect(connectionInfo) // connector
            dbm.closeResourceOnExit(dialect)
            if (!(dialect instanceof JdbcDialect)) {
                logger.info("Skipping checks for connection ${connectionInfo.getName()} as it is not a database one")
                return
            }

            connection = connector.getConnection()
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
            

            //def dialect = connector.connect() //filter==null ? 
                                       //: filter.filter(connectionInfo, connector.connect(), connection)
        
            checkSet.check.each { checkXML ->
                def checkName = checkXML.@name
                if (checkName==null) {
                    logger.error("Check name is not defined")
                    return
                }
                
                logger.info ("Running check ${checkName} for ${context.serverName} ")
                
                // dbmSync.loadRecords("Server=\"${context.serverName}\" && CheckID=\"${checkName}\"")

                try {
                    // TODO check if source does not exist
                    def check = checks[checkName]
                    if (check==null) {
                        check = getClassInstance("io/dbmaster/dbstyle/checks/${checkName}.groovy")
                        if (check==null) {
                            throw new Exception("Implementation for check ${checkName} was not found")
                        }
                        check.init(context)
                        check.name = checkName
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
                        def link = tools.linkToObject(issue.object_type, context.serverName, issue.object_name)
                        issueIndex = issueIndex + 1
                        println """
                           <tr>
                             <td>${issueIndex}</td>
                             <td>${connectionInfo.getName()}</td>
                             <td>${environment ?: ""}</td>
                             <td>${checkName}</td>
                             <td>${issue.object_type}</td>
                             <td><a href="${link}">${issue.object_name}</a></td>
                             <td>${issue.severity}</td>
                             <td>${issue.description}</td>
                             <td>${issue.object_key == null ? "" :  context.serverName + "."+issue.object_key}</td></tr>"""
                    }
                    // def statistics = dbmSync.completeSync()
                } catch (Exception e) {
                    def msg = "Cannot check ${checkName} for ${context.serverName}: ${e.getMessage()}"
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