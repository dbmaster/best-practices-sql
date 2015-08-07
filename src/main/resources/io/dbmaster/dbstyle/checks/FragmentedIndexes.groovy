package io.dbmaster.dbstyle.checks

import io.dbmaster.dbstyle.api.*
import java.sql.Connection
import groovy.sql.Sql
import com.branegy.dbmaster.connection.Dialect

public class FragmentedIndexes extends Check {
    def formatter =  new java.text.DecimalFormat("#.##")

    def format = { value -> formatter.format(value) }

    public void check(Connection connection, Dialect dialect) {
        def min_fragmentation = getParameter("min_fragmentation")
        if (min_fragmentation==null) {
           min_fragmentation = 30.0
        }
        def min_page_count = getParameter("min_page_count")
        if (min_page_count==null) {
           min_page_count = 500
        }
        

        def params = [ min_page_count, min_fragmentation ]
        logger.info("Parameters = ${params}")

        dialect.getDatabases().each { dbInfo ->
            def database_name =  dbInfo.getName()
            
            if (!context.isObjectInScope("Database",context.serverName+"."+database_name)) {
                logger.debug("Database ${database_name} is out of scope")
                return
            }

            def cL = dbInfo.getCustomData("Compatibility Level");
            if (Integer.parseInt(cL) <= 80) {
                logger.warn("""Database ${database_name} compatibility level
                             (${cL}) is below or equal 80. Skipping the check""")
                return
            }

            if (dbInfo.getCustomData("State") != "ONLINE") {
                logger.warn("Database ${database_name} state is "+
                            "${dbInfo.getCustomData("State")}. Skipping the check")
                return
            } else {
                logger.debug("Checking database ${database_name}")
            }

            try {

                def query = """ USE [${database_name}];
                                SELECT 
                                    objects.type object_type,
                                    schemas.name schema_name,
                                    objects.name object_name,
                                    indexes.name index_name,
                                    indexes.is_disabled index_disabled,
                                    MAX(ips.avg_fragmentation_in_percent) as fragmentation,
                                    SUM(ips.page_count) as page_count
                                FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED')  ips
                                INNER JOIN sys.objects objects ON ips.object_id = objects.object_id
                                INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id
                                INNER JOIN sys.indexes indexes ON indexes.object_id = objects.object_id 
                                WHERE objects.type IN ('U','V') 
                                    AND objects.is_ms_shipped = 0
                                    /* AND indexes.is_disabled = 0 */
                                    AND indexes.is_hypothetical = 0 
                                    AND indexes.[type] IN(1,2,3,4)
                                    AND ips.alloc_unit_type_desc = 'IN_ROW_DATA' 
                                    AND ips.index_level = 0
                                GROUP BY objects.type, schemas.name, objects.name,indexes.name,indexes.is_disabled
                                HAVING SUM(ips.page_count)>=?
                                    AND MAX(ips.avg_fragmentation_in_percent)>=?
                """

                def sql = new Sql(connection)
                sql.rows(query, params).each {
                    addMessage(
                       [ "object_type" : "Database",
                         "object_name" : database_name,
                         "severity"    : "warning",
                         "object_key"  : database_name+"."+it.schema_name+"."+it.object_name+"."+it.index_name,
                         "description" : "Index " + it.index_name + " fragmentation " + format(it.fragmentation) + " for "+
                                          (it.object_type.trim().equals("U") ? "table" : "view") + " "+ it.schema_name + "." + it.object_name +
                                          " is above threshold of "+min_fragmentation+"%"
                       ])
                }
            } catch (Exception e) {
                def msg = "Cannot check fragmentation for database ${database_name}. ${e.getMessage()}"
                org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
                logger.error(msg)
            }
        }
    }
}
