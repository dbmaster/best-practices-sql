package io.dbmaster.dbstyle.api

import com.branegy.scripting.DbMaster
import org.slf4j.Logger

public class Context {
    
    public Logger logger
    
    public DbMaster dbm
   
    List<Suppression> suppressions
    
    List<Scope> scopeList
    
    public String serverName
    
    public boolean isObjectInScope(String objectType, String objectKey) {
        for (Scope scope: scopeList) {
            for (ObjectFilter filter: scope.filters) {
                if (!filter.isObjectInScope(objectType, objectKey)) {
                    return false
                }
            }
        }
        return true
    }
}