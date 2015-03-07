package io.dbmaster.dbstyle.api;

import java.util.TreeSet;

public final class Message {

    String object_type;

    String object_name;

    String severity;

    String description;

    /* Useful when copying list for further troubleshooting / automation */
    String object_key;
    

    public Message(Map properties=null) {
       properties?.each { k,v ->  this."$k" = v }
    }

}
