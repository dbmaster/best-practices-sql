package io.dbmaster.dbstyle.filter

import com.branegy.dbmaster.model.DatabaseInfo

import io.dbmaster.dbstyle.api.ObjectFilter
import io.dbmaster.tools.DbmTools

import java.sql.Connection
import org.slf4j.Logger

import com.branegy.service.connection.model.DatabaseConnection
import com.branegy.dbmaster.connection.Dialect
import com.branegy.inventory.api.InventoryService
import com.branegy.service.connection.api.ConnectionService
import com.branegy.service.core.QueryRequest
import com.branegy.inventory.model.DatabaseUsage



public class InventoryFilter extends ObjectFilter {
        
    public static final boolean INCLUDE = true
    public static final boolean EXCLUDE = false    
    
    boolean includeExclude
    String  objectName
    String  filter
    List<Object> objects
    
    public InventoryFilter(String objectName, String filter, boolean includeExclude) {
        this.objectName = objectName
        this.filter
        this.includeExclude = includeExclude
    }
   
    public void init(DbmTools tools, Map<String,Object> properties) {
        def dbm = tools.dbm        
        
        if (objectName.equals("servers")) {
            def service = dbm.getService(ConnectionService.class)
            objects = service.getConnectionSlice(new QueryRequest(filter), null)    
        } else {
            // InventoryService inventorySrv = dbm.getService(InventoryService.class)
            // objects = inventorySrv.getApplicationList(new QueryRequest(filter))    
        }
    }
    
    public boolean isObjectInScope(Object object) {
        //if (object instanceof Database) {
        //    return filteredDBs.contains(object)
        //} else {
            return true
        //}
    }

}