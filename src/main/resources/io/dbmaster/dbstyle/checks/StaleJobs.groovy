package io.dbmaster.dbstyle.checks

import io.dbmaster.dbstyle.api.*
import java.sql.Connection
import groovy.sql.Sql

// TODO Add check into


public class StaleJobs extends Check {
    
    public static String CHECK_QUERY = """
        WITH jobs 
        AS (
            SELECT
                @@SERVERNAME    server_name,                
                j.job_id        job_id,
                j.name          job_name,
                j.description   job_description,
                j.date_created  date_created,
                j.date_modified date_modified,
                CAST(j.enabled as bit) job_enabled,
                CAST(CASE WHEN ja.run_requested_date IS NOT NULL AND ja.stop_execution_date IS NULL 
                         THEN 1
                         ELSE 0 
                    END AS bit) running,
                c.name AS category,
                COALESCE(ja.start_execution_date, msdb.dbo.agent_datetime(jh.run_date, jh.run_time)) last_start_date,
                ja.next_scheduled_run_date next_run_date
            FROM msdb.dbo.sysjobs j
            JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
            OUTER APPLY (SELECT TOP 1 *
                         FROM msdb.dbo.sysjobactivity ja
                         WHERE j.job_id = ja.job_id
                         ORDER BY ja.run_requested_date DESC) ja
            LEFT JOIN msdb.dbo.sysjobhistory jh  ON j.job_id = jh.job_id AND ja.job_history_id = jh.instance_id
            LEFT JOIN msdb.dbo.sysjobsteps s ON ja.job_id = s.job_id AND ja.last_executed_step_id = s.step_id
        )
        SELECT
            *, 
            DATEDIFF(DAY, date_created, GETDATE())    as created_days_ago,
            DATEDIFF(DAY, last_start_date, GETDATE()) as run_days_ago,
            'sp_delete_job @job_id =''' + CONVERT(VARCHAR(40),job_id)+ ''', @delete_history = 1, @delete_unused_schedule=1'
             +CHAR(13)+CHAR(10)+'GO'+CHAR(13)+CHAR(10) AS script
        FROM jobs
        WHERE ( last_start_date IS NULL OR DATEDIFF(DAY, last_start_date, GETDATE()) >= :p_started_days_ago )
          AND DATEDIFF(DAY, date_modified, GETDATE()) >= :p_modified_days_ago
          -- AND category <> 'exclude category' 
          -- AND job_name not like 'exclude name'
        ORDER BY job_name, last_start_date""".toString()    


    public void check(Connection connection, dialect) {
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

        try {

            def sql = new Sql(connection)
            sql.rows(query, params).each {
                addMessage(
                   [ "object_type" : "Job",
                     "object_name" : it.job_name,
                     "severity"    : "warning",
                     "object_key"  : database_name+"."+it.schema_name+"."+it.object_name+"."+it.index_name,
                     "description" : "Index " + it.index_name + " defragmentation " + format(it.fragmentation) + " for "+
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