import com.branegy.dbmaster.model.DatabaseInfo

import org.dbmaster.dbstyle.api.InputFilter
import java.sql.Connection
import java.util.Map
import org.slf4j.Logger

import com.branegy.service.connection.model.DatabaseConnection
import com.branegy.dbmaster.connection.Dialect

import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map.Entry

import com.branegy.inventory.api.InventoryService
import com.branegy.inventory.model.Database
import com.branegy.service.core.QueryRequest
import com.branegy.inventory.model.DatabaseUsage
import com.branegy.inventory.model.Application
import com.branegy.inventory.model.ContactLink
import com.branegy.inventory.model.Contact
import com.branegy.inventory.api.ContactLinkService
import com.branegy.service.base.api.ProjectService

public class ApplicationFilter implements InputFilter {
    
    final Map<String,Map<String,String>> values = [:]
    Logger logger
    String manufacturer = "TODO"
   
    public void init(Map<String,Object> config) {
        def dbm = config.get("dbm")
        logger = config.get("logger")
        
        def p_query = "Deleted=no"
        def db_fields = [] //"Deleted"
        def app_fields = ["Manufacturer"]
        def con_fields = []
        
        InventoryService inventorySrv = dbm.getService(InventoryService.class)
        def inventoryDBs = new ArrayList(inventorySrv.getDatabaseList(new QueryRequest(p_query)))
        inventoryDBs.sort { it.getServerName()+"_"+it.getDatabaseName()  }
        String projectName =  dbm.getService(ProjectService.class).getCurrentProject().getName()
        def db2AppsLinks = inventorySrv.getDBUsageList()
        def dbApps = db2AppsLinks.groupBy { it.getDatabase() }
        def contactLinks = dbm.getService(ContactLinkService.class).findAllByClass(Application.class,null)
        def appId2contactLink = contactLinks.groupBy{ contactLink-> contactLink.getApplication().getId()}

        for (Database database: inventoryDBs) {
            def item = [connection:database.getServerName(), database:database.getDatabaseName()]
            values.put(database.getServerName()+"."+database.getDatabaseName(),item)
            
            def apps = dbApps[database]
            if (apps == null || apps.isEmpty()) {
                apps = Collections.singletonList(null)
            }
            for (DatabaseUsage dbusage: apps) {
                def app = dbusage!=null ? dbusage.getApplication() : null
                def contactLinkList = app!=null ? appId2contactLink.get(app.getId()): null
                if (contactLinkList == null || contactLinkList.isEmpty()) {
                    contactLinkList = Collections.singletonList(null)
                }
                for (ContactLink contactLink:contactLinkList) {
                    db_fields.each { fieldName ->  item.put("db."+fieldName,database.getCustomData(fieldName)) }
                    if (app!=null) {
                        app_fields.each { fieldName -> item.put("app."+fieldName,app.getCustomData(fieldName)) }
                    }
                    if (contactLink!=null) {
                        def contact = contactLink.getContact()
                        con_fields.each { fieldName -> item.put("c."+fieldName,contact.getCustomData(fieldName)) }
                    }
                }
            }
            logger.debug(""+item)
        }
    }
    
    public boolean accept(DatabaseConnection connection) {
        return true;
    }

    Dialect filter(DatabaseConnection connection, Dialect dialect, Connection c) {
        List<DatabaseInfo> list = dialect.getDatabases()
        .findAll{ it ->
              def item = values.get(connection.getName()+"."+it.name)
              if (item == null) {
                  logger.warn("Database {}.{} does not exist in the inventory", connection.getName(), it.name)
                  return true
              } else {
                  boolean result = manufacturer.equals(item.get("app.Manufacturer"))
                  logger.debug("include:{}.{}:{}", connection.getName(), it.name, result)
                  return result
              }                  
        }; 
        return new Dialect() {
            public List<DatabaseInfo> getDatabases() {
                return list
            }
            public void close() {
                dialect.close()
            }
            public DatabaseConnection getCI(){
                return dialect.getCI()
            }
        };
    }
    
    public void destroy(){
    }
}