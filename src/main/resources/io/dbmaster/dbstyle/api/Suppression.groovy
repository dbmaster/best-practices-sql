package io.dbmaster.dbstyle.api

import java.util.regex.Pattern

public class Suppression {
    
    public static enum SuppressionType { 
        ALL, 
        PREFIX, 
        EXACT_VALUE, 
        PATTERN 
    }
    
    public String check
    
    public SuppressionType type
    
    public Pattern pattern
    
    public String key
    
    public Suppression (String check, String key) {
        if (check==null || check.length()==0) {
            throw new IllegalArgumentException("Check cannot be null")
        }
        if (key==null || key.length()==0) {
            throw new IllegalArgumentException("Key cannot be null")
        }        
        this.check = check
        def idx = key.indexOf('*')
        if (key.equals("*") || key.equalsIgnoreCase("all")) {
            type = SuppressionType.ALL;
        } else if ( idx == key.length()-1 ) {
            type = SuppressionType.PREFIX
            this.key = key.substring(0,key.length()-1)
        } else if ( idx == -1 ) {
            type = SuppressionType.EXACT_VALUE
            this.key = key
        } else {
            type = SuppressionType.PATTERN
            key = key.replaceAll('([\\\\\\.\\[\\{\\(\\)\\+\\?\\^\\$\\|])', '\\\\$1')
            this.key = key
            this.pattern= ~"${key.replaceAll('(\\*)','\\.\\*')}"            
        }
    }
    
    public boolean checkKey(String matchKey) {
         def result = false
         switch (this.type) {
            case SuppressionType.ALL:
                result = true
                break
            case SuppressionType.PREFIX:
                result = matchKey!=null && matchKey.startsWith(key)
                break
            case SuppressionType.EXACT_VALUE:
                result = matchKey!=null && matchKey.equalsIgnoreCase(key)
                break
            case SuppressionType.PATTERN:
                result = matchKey!=null && pattern.matcher(matchKey).matches()
                break
            default:
                throw new IllegalArgumentException("Unknown type value "+type)
                break
        }   
    }
    
}
