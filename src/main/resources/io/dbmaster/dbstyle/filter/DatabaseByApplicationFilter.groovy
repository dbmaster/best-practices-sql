package io.dbmaster.dbstyle.filter

import io.dbmaster.dbstyle.api.ObjectFilter
import io.dbmaster.tools.DbmTools

import com.branegy.inventory.api.InventoryService
import com.branegy.inventory.model.Database
import com.branegy.service.core.QueryRequest

public class DatabaseByApplicationFilter extends ObjectFilter {
    
    final List<Database> filteredDBs = []
   
    public void init(DbmTools tools, Map<String,Object> properties) {
        def dbm = tools.dbm
        def appFilter =  properties.get("filter")
        tools.logger.debug("DatabaseByApplicationFilter.Filter = ${appFilter}")
        InventoryService inventorySrv = dbm.getService(InventoryService.class)
        
        def filteredApps = inventorySrv.getApplicationList(new QueryRequest(appFilter))
        
        def dbLinks = inventorySrv.getDBUsageList()
        dbLinks.each { link ->
            if (filteredApps.contains(link.application) && !link.database.deleted) {
                def name = link.database.connectionName +"."+link.database.databaseName
                tools.logger.debug("Include database into scope: ${name} app: ${link.application.applicationName}")
                filteredDBs.add (name)
            }
        }
    }
    
    public boolean isObjectInScope(String objectType, String objectKey) {
        if (objectType.equals("Database")) {
            return filteredDBs.contains(objectKey)
        } else {
            return true
        }
    }
}