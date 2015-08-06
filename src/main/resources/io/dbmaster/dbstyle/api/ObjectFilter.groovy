package io.dbmaster.dbstyle.api

import java.sql.Connection

import io.dbmaster.tools.DbmTools

public abstract class ObjectFilter {

    public abstract void init(DbmTools tools, Map<String,Object> properties)
    
    public abstract boolean isObjectInScope(Object o)
    
    public void destroy() {}
}