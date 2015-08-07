package io.dbmaster.dbstyle.filter

import io.dbmaster.dbstyle.api.ObjectFilter
import io.dbmaster.tools.DbmTools

import com.branegy.service.connection.model.DatabaseConnection
import com.branegy.dbmaster.connection.Dialect
import com.branegy.inventory.api.InventoryService
import com.branegy.service.connection.api.ConnectionService
import com.branegy.service.core.QueryRequest

public class InventoryFilter extends ObjectFilter {
        
    public static final boolean INCLUDE = true
    public static final boolean EXCLUDE = false    
    
    boolean includeExclude
    String  objectName
    String  filter
    List<Object> objectKeys
    def logger
    
    public InventoryFilter(String objectName, String filter, boolean includeExclude) {
        this.objectName = objectName
        this.filter = filter
        this.includeExclude = includeExclude
    }
   
    public void init(DbmTools tools, Map<String,Object> properties) {
        def dbm = tools.dbm        
        logger = tools.logger
        logger.info("Initialize filter ${objectName} ${filter} ${includeExclude}")
        if (objectName.equals("servers")) {
            def service = dbm.getService(ConnectionService.class)
            objectKeys = service.getConnectionSlice(new QueryRequest(filter), null).collect { it.name }
            objectKeys.each { tools.logger.debug("Included server ${it}") }
        } else {
            // InventoryService inventorySrv = dbm.getService(InventoryService.class)
            // objects = inventorySrv.getApplicationList(new QueryRequest(filter))    
        }
    }
    
    public boolean isObjectInScope(String objectType, String objectKey) {
        if (objectName.equals("servers") && objectType.equals("DatabaseConnection")) {
            // TODO - below implements only inclusions
            return objectKeys.contains(objectKey)
        } else {
            return true
        }
    }

}