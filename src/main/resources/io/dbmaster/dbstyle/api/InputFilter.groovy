package io.dbmaster.dbstyle.api

import java.sql.Connection
import java.util.Map

import com.branegy.service.connection.model.DatabaseConnection
import com.branegy.dbmaster.connection.Dialect

public interface InputFilter {
    void init(Map<String,Object> config);
    
    boolean accept(DatabaseConnection connection);
    
    Dialect filter(DatabaseConnection connection, Dialect dialect, Connection c);
    
    void destroy();
}
