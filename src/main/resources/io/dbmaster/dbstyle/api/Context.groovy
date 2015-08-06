package io.dbmaster.dbstyle.api

import com.branegy.scripting.DbMaster

public class Context {
    
    public def logger
    
    public DbMaster dbm
   
    List<Suppression> suppressions
    
    List<Scope> scopeList
    
    public String serverName
    
    public boolean isObjectInScope(Object o) {
        for (Scope scope: scopeList) {
            for (ObjectFilter filter: scope.filters) {
                if (!filter.isObjectInScope(o)) {
                    return false
                }
            }
        }
        return true
    }
}