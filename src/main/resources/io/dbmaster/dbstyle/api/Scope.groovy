package io.dbmaster.dbstyle.api

public class Scope {
    
    String name
     
    List<ObjectFilter> filters = []
    
    public Scope(String name) {
        this.name = name
    }
    
    public void addFilter (ObjectFilter filter) { 
       filters.add(filter)
    }
}