package io.dbmaster.dbstyle.checks

import io.dbmaster.dbstyle.api.*
import static io.dbmaster.dbstyle.api.SeverityLevel.*
import java.sql.Connection
import groovy.sql.Sql
import com.branegy.dbmaster.connection.Dialect

public class UserObjectSystemDB extends Check {

    def CHECK_QUERY = { dbName ->
          """select 
            s.name as schema_name,
            o.name as object_name,
            o.type,
            o.type_desc,
            o.create_date,
            o.modify_date
          from ${dbName}.sys.objects o
          inner join ${dbName}.sys.schemas s on o.schema_id = s.schema_id
          where o.is_ms_shipped=0 and o.type not in ('C','D','F','PK','UQ')
          order by s.name, o.name""" }
          

    public void check(Connection connection, Dialect dialect) {
        def ignoreList = getParameter("sysdb.valid_object")
        if (ignoreList==null) {
            ignoreList = []
        } else if (!(ignoreList instanceof List)) {
            ignoreList = [ ignoreList ]
        }
        ignoreList = ignoreList.collect { it -> it.trim().toUpperCase() }
        logger.debug("Ignore List = ${ignoreList.join(",")}")
        def params = [ ]
        def sql = new Sql(connection)
        ["msdb", "master", "model"].each { dbName->
            sql.rows(CHECK_QUERY(dbName), params).each {
                def objectName = it.schema_name+"."+it.object_name
                if (!ignoreList.contains(dbName.toUpperCase()+"."+objectName.toUpperCase())) {
                
                    addMessage(
                       [ "object_type" : "Database",
                         "object_name" : dbName,
                         "object_key"  : dbName+"."+objectName,
                         "severity"    : WARNING,
                         "description" : "Non system object ${objectName} (${it.type_desc}) exists in ${dbName} database"
                       ])
                
                }
            }
        }
   
    }
}