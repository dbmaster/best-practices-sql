import com.branegy.service.connection.api.ConnectionService
import io.dbmaster.api.groovy.DbmTools
import io.dbmaster.dbstyle.checks.StaleJobs
import groovy.sql.Sql

connectionSrv = dbm.getService(ConnectionService.class)

def dbConnections
if (p_servers!=null && p_servers.size()>0) {
    dbConnections = p_servers.collect { serverName -> connectionSrv.findByName(serverName) }
} else {
    dbConnections  = connectionSrv.getConnectionList().findAll { it.driver!="ldap" }
}

def tools = new DbmTools ( dbm, logger, getBinding().out)

if (p_action=="List Jobs") {
    println """<table cellspacing="0" class="simple-table" border="1">
                    <tr style="background-color:#EEE">
                        <th>server_name</th>
                        <th>job_id</th>
                        <th>job_name</th>
                        <th>job_description</th>
                        <th>date_created</th>
                        <th>date_modified</th>
                        <th>job_enabled</th>
                        <th>running</th>
                        <th>category</th>
                        <th>last_start_date</th>
                        <th>next_run_date</th>
                        <th>created_days_ago</th>
                        <th>run_days_ago</th>
                        <th>script</th>
                    </tr> """
}

Closure filter = null
if (p_filter!=null) {
    filter = new GroovyShell().evaluate("{ job -> "+ p_filter +"  }")
}
dbConnections.each { connectionInfo ->
    try {
    
        connection = tools.getConnection ( connectionInfo.name ) 
        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
        
        def sql = new Sql(connection)                
        def rows = sql.rows (parameters, StaleJobs.CHECK_QUERY)
        if ( filter!=null ) {
            rows = rows.findAll (filter)
        }
        if (p_action=="List Jobs") {
            rows.each { row ->
                print "<tr>"
                
                print "<td>${ tools.rsToString(row.job_name) }</td>"
                print "<td>${ tools.rsToString(row.job_description) }</td>"
                print "<td>${ tools.rsToString(row.date_created) }</td>"
                print "<td>${ tools.rsToString(row.date_modified) }</td>"
                print "<td>${ tools.rsToString(row.job_enabled) }</td>"
                print "<td>${ tools.rsToString(row.running) }</td>"
                print "<td>${ tools.rsToString(row.category) }</td>"
                print "<td>${ tools.rsToString(row.last_start_date) }</td>"
                print "<td>${ tools.rsToString(row.next_run_date) }</td>"
                print "<td>${ tools.rsToString(row.created_days_ago) }</td>"
                print "<td>${ tools.rsToString(row.run_days_ago) }</td>"
                
                // last_start_date 	next_run_date 	created_days_ago 	run_days_ago
                // row.each  { cell -> 
                //    print "<td>${ tools.rsToString(cell.value) }</td>"
                // }
                println "</tr>"
            }
        }
         // execute(Map params, String sql, Closure processResults)}
    } catch (Exception e) {
        def msg = "Cannot retrieve job information"
        logger.error(msg, e)
    }
}
if (p_action=="List Jobs") {
    println "</table>"
}