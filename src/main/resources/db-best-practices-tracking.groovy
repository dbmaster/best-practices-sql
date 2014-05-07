import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry

import ExcelSync.KeyWrapper;

import com.branegy.service.base.api.ProjectService;
import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcConnector
import com.branegy.email.EmailSender

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.text.SimpleDateFormat;

def toURL = { link ->
    link==null ? "NULL" : link.encodeURL().replaceAll("\\+", "%20")
}
String.metaClass.encodeURL = { java.net.URLEncoder.encode(delegate) }

String projectName =  dbm.getService(ProjectService.class).getCurrentProject().getName();

def acceptable_severity = {input_severity ->
    def acceptable_levels = []

    switch(input_severity){
        case "":
        case "info":
        case null:
            acceptable_levels.addAll(['error', 'warning', 'info']);
            break;
        case "warning":
            acceptable_levels.addAll(['error', 'warning'])
            break;
        case "error":
            acceptable_levels.addAll(['error']);
            break;
    }

    return acceptable_levels;
}

def linkToObject = { type, serverName, objectName  ->
    prefix = "#inventory/project:${toURL(projectName)}"
    if (type.equals("Application")) {
        return "${prefix}/applications/application:${toURL(objectName)}"
    } else if (type.equals("Server")) {
        return  "${prefix}/servers/server:${toURL(serverName)}"
    } else if (type.equals("Database")) {
        return  "${prefix}/databases/server:${toURL(serverName)},db:${toURL(objectName)}"
    } else if (type.equals("Connection")) {
        // TODO here there should be a link to server
        return  "${prefix}/databases/search:ServerName=${toURL(serverName)}" 
    } else if (type.equals("Job")) {
        // TODO we do not have any information about jobs in dbmaster yet
        return  "${prefix}/databases" 
    } else {
        throw new RuntimeException("Object type ${type} was not expected")
    }
}

def required_severity_levels = acceptable_severity(p_severity)

// -----------------Rules---------------------------------------------------------------------------
connectionSrv = dbm.getService(ConnectionService.class)

def dbConnections
if (p_servers!=null && p_servers.size()>0) {
    dbConnections = p_servers.collect { serverName -> connectionSrv.findByName(serverName) }
} else {
    dbConnections  = connectionSrv.getConnectionList()
}
def modules
if (p_modules!=null && p_modules.size()>0) {
    modules = p_modules 
} else {
    modules  = ["FragmentedIndexes"]
}
println  """
   <table class="simple-table" cellspacing="0" cellpadding="10">
        <tr style="background-color:#EEE">
            <td>Server</td>
            <td>Environment</td>
            <td>CheckID</td>
            <td>Object Type</td>
            <td>Object Name</td>
            <td>Severity</td>
            <td>Issue</td>
            <td>TechKey</td>
        </tr>
"""

ExcelSync excelSync = new ExcelSync(
    ["Server","Environment","CheckID","Object Type","Object Name","Severity","Issue","TechKey","Status"],   
    ["Server","CheckID","Object Type","Object Name","TechKey"],
    "Status", 
    "New", 
    "AutoClosed", 
    ["Ignore"] as Set,
    ["Ignore","AutoClosed","Closed"] as Set,
    new File(p_tracking_file),
    logger,
    p_backup
);

dbConnections.each { connectionInfo ->
try {    
    connector = ConnectionProvider.getConnector(connectionInfo)
    def serverName = connectionInfo.getName()
    if (!(connector instanceof JdbcConnector)) {
        logger.info("Skipping checks for connection ${connectionInfo.getName()} as it is not a database one")
        return
    }
    connection = connector.getJdbcConnection(null)
    connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
    dbm.closeResourceOnExit(connection)

    modules.each { module ->
        logger.info ("Running check ${module} for ${connectionInfo.getName()} ")

        try {
            //def checkClass = new GroovyClassLoader().loadClass("org.dbmaster.dbstyle.checks.${module}")
            
            ClassLoader cl = this.getClass().getClassLoader();
            def sourceCode = new GroovyCodeSource(cl.getResource("org/dbmaster/dbstyle/checks/${module}.groovy"))
            def checkClass = new GroovyClassLoader(cl).parseClass(sourceCode)
            
            def check = checkClass.newInstance()
            check.init()
            check.logger = logger
            def dialect = connector.connect()
            def collector = new org.dbmaster.dbstyle.api.MessageCollector()
            check.setMessageCollector(collector)
            
            def parameters = [:]
            parameters["dbmaster"] = dbm
            if (p_properties!=null) {
                p_properties.replaceAll("\r\n", "\n").split("\n").each {
                    def key_value = it.split("=")
                    if (key_value.length>=2) {
                        def value = key_value[1..key_value.length-1].join("=")
                        parameters[key_value[0]] = value
                        logger.info("Setting property ${key_value[0]} to ${value}")
                    } else {                        System.out.println("Setting property ${key_value[0]} to '${value}'")

                        logger.error("Cannot recognize property ${it}")
                    }
                }
            }
            
            check.setParameters(parameters)
            check.check(connection, dialect)

            // -----------------Layout ------------------------------------------------------------------

            collector.messages.sort { it -> it.object_type + it.object_name }

            collector.messages.each { issue ->

               if (!required_severity_levels.contains(issue.severity)){
                   return;
               }
                def environment = connectionInfo.getProperties().find{it.key=="environment"}?.value
                
                excelSync.addRow(connectionInfo.getName(), environment ?: "", module, issue.object_type, issue.object_name,
                    issue.severity, issue.description, issue.object_key == null ? "" :  serverName + "."+issue.object_key );
                
                println """
               <tr>
                 <td>${connectionInfo.getName()}</td>
                 <td>${environment ?: ""}</td>
                 <td>${module}</td>
                 <td>${issue.object_type}</td>

                 <td><a href="${linkToObject(issue.object_type, serverName, issue.object_name)}">${issue.object_name}</a></td>
                 <td>${issue.severity}</td>
                 <td>${issue.description}</td>
                 <td>${issue.object_key == null ? "" :  serverName + "."+issue.object_key}</td></tr>"""
            }
        } catch (Exception e) {
            def msg = "Cannot check ${module} for ${serverName} "+e.getMessage()
            org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
            logger.error (msg)
        }
    }
} catch (Exception e) {
    def msg = "Error occurred "+e.getMessage()
    org.slf4j.LoggerFactory.getLogger(this.getClass()).error(msg,e);
    logger.error (msg)
}

}
println "</table>"

int[] score = excelSync.syncAndReturnScore();

println "During update ${score[0]} new issue(s) were found and ${score[1]} were automatically closed. Total open issues ${score[2]}<br/>"+
 "Review issues at <a href='${new File(p_tracking_file).toURI()}'>${p_tracking_file}</a>";
if (p_email != null){
    EmailSender sender = dbm.getService(EmailSender.class);
    String msg = 
"""
Best practices tracking worksheet was automatically updated by DBMaster.

During update ${score[0]} new issue(s) were found and ${score[1]} were automatically closed.

Total open issues ${score[2]}

Review issues at ${p_tracking_file}.

---------------------------------------
This message was automatically generated by DBMaster plugin
""";
    
    sender.createMessage(p_email, "SQL Server best practices: worksheet updated", msg, false);
    sender.sendMessage();
}

logger.info("Check is completed")

class ExcelIterator implements Closeable{
    final File file;
    final List<String> columnList;
    Row row;
    
    public ExcelIterator(File file, List<String> headerColumnList) throws IOException{
        this.file = file;
        InputStream is = null;
        try {
            Workbook workbook;
            if (!file.exists()){
                workbook = new XSSFWorkbook();
                Sheet sheet = workbook.createSheet();
                row = sheet.createRow(0);
                int i=0;
                for (String c:headerColumnList){
                    row.createCell(i++).setCellValue(c);
                }
            } else {
                is = FileUtils.openInputStream(file);
                workbook = new XSSFWorkbook(is);
            }
            
            Sheet sheet = workbook.getSheetAt(0);
            row = sheet.getRow(0);
            List<String> list = new ArrayList<String>();
            for (Cell cell:row){
                String stringCellValue = cell.getStringCellValue();
                if (stringCellValue == null){
                    throw new IllegalStateException("Cell header cell can't be null at index "+cell.getRowIndex()+":"+cell.getColumnIndex());
                }
                list.add(stringCellValue);
            }
            columnList = Collections.unmodifiableList(list);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
   
    public boolean hasMoreRow() {
        return row.getRowNum()<row.getSheet().getLastRowNum();
    }
    public void nextRow() {
        if (!hasMoreRow()){
            row = row.getSheet().createRow(row.getRowNum()+1);
        } else {
            row = row.getSheet().getRow(row.getRowNum()+1);
        }
    }
    public Object getColumn(int index) {
        Cell cell = row.getCell(index);
        return cell == null ? null : cell.getStringCellValue();
    }
    public void setColumn(int index, Object object) {
        Cell cell = row.getCell(index);
        if (cell == null){
            cell = row.createCell(index);
        }
        cell.setCellValue((String)object);
    }
    public List<String> getColumnList() {
        return columnList;
    }
    

    public void close() throws IOException {
        OutputStream os = null;
        try{
            os = FileUtils.openOutputStream(file);
            row.getSheet().getWorkbook().write(os);
        } finally {
            IOUtils.closeQuietly(os);
        }
    }
    
}

class ExcelSync{
    private final File file;
    
    private final List<String> columnList;
    private final List<String> keyColumnList;
    private final String statusColumn;
    private final String newStatusStatus;
    private final String autoCloseStatus;
    private final Set<String> ignoreAutoCloseStatusSet;
    private final Set<String> nonOpenStatusSet;
    private final Logger logger;
    private final boolean backup;
    
    private final int[] keyIndex;
    private final int[] columnIndex;
    
    private Map<KeyWrapper, Object[]> existsRecords;
    
    static class KeyWrapper{
        final Object[] keys;

        public KeyWrapper(Object[] keys) {
            this.keys = keys;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof KeyWrapper)){
                return false;
            }
            return Arrays.equals(keys, ((KeyWrapper) obj).keys);
        }
    }
    
    public ExcelSync(List<String> columnList, List<String> keyColumnList, String statusColumn,
            String newStatusStatus,
            String autoCloseStatus,
            Set<String> ignoreAutoCloseStatusSet,
            Set<String> nonOpenStatusSet,
            File file,
            Logger logger, boolean backup){
        
        if (!columnList.containsAll(keyColumnList)){
            throw new IllegalArgumentException(columnList+" doesn't containt "+keyColumnList);
        }
        if (!columnList.contains(statusColumn)){
            throw new IllegalArgumentException(columnList+" doesn't containt "+statusColumn);
        }
        if (keyColumnList.contains(statusColumn)){
            throw new IllegalArgumentException(keyColumnList+" containts "+statusColumn);
        }
        
        keyIndex = new int[keyColumnList.size()];
        for (int i=0; i<keyColumnList.size(); ++i){
            String c = keyColumnList.get(i);
            keyIndex[i] = columnList.indexOf(c);
        }
        
        int j = 0;
        columnIndex = new int[columnList.size()-keyColumnList.size()-1];
        for (int i=0; i<columnList.size(); ++i){
            String c = columnList.get(i);
            if (keyColumnList.contains(c) || c.equals(statusColumn)){
                continue;
            }
            columnIndex[j++] = i;
        }
        existsRecords = new LinkedHashMap<KeyWrapper, Object[]>();
        this.file = file;
        this.columnList = columnList;
        this.keyColumnList = keyColumnList;
        this.statusColumn = statusColumn;
        this.newStatusStatus = newStatusStatus;
        this.autoCloseStatus = autoCloseStatus;
        this.ignoreAutoCloseStatusSet = ignoreAutoCloseStatusSet;
        this.nonOpenStatusSet = nonOpenStatusSet;
        this.logger = logger;
        this.backup = backup;
    }
    
    public void addRow(Object... args){
        Object[] keys = new Object[keyIndex.length];
        for (int i=0; i<keyIndex.length; ++i){
            keys[i] = args[keyIndex[i]];
        }
        Object[] values = new Object[columnIndex.length];
        for (int i=0; i<columnIndex.length; ++i){
            values[i] = args[columnIndex[i]];
        }
        if (existsRecords.put(new KeyWrapper(keys),values)!=null) {
            logger.warn("Key already exists {}", Arrays.toString(keys))
        }
    }
    
    public int[] syncAndReturnScore(){
        if (backup && file.exists()){
            FileUtils.copyFile(file, new File(file.getParentFile(), 
                FilenameUtils.getBaseName(file.getName())
                  + "_"+new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
                  + "."+FilenameUtils.getExtension(file.getName())));
        }
        ExcelIterator eit = null;
        try {
            eit = new ExcelIterator(file, columnList);
            if (!eit.getColumnList().containsAll(columnList)){
               throw new IllegalArgumentException(eit.getColumnList()+" doesn't containt "+columnList);
            }
            
            // prepare keys
            int[] keyIndexExcel = new int[keyIndex.length];
            for (int i=0; i<keyColumnList.size(); ++i){
                String c = keyColumnList.get(i);
                keyIndexExcel[i] = eit.getColumnList().indexOf(c);
            }
            // prepare status column
            int statusColumnIndexExcel = eit.getColumnList().indexOf(statusColumn);
            // prepare values
            int j = 0;
            int[] columnIndexExcel = new int[columnList.size()-keyColumnList.size()-1];
            for (int i=0; i<columnList.size(); ++i){
                String c = columnList.get(i);
                if (keyColumnList.contains(c) || c.equals(statusColumn)){
                    continue;
                }
                columnIndexExcel[j++] = eit.getColumnList().indexOf(c);
            }
            
            Object[] keys = new Object[keyIndexExcel.length];
            int newScore = 0;
            int autoClosedScore = 0;
            int openScore = 0;
            while (eit.hasMoreRow()){
                eit.nextRow();
                for (int i=0; i<keys.length; ++i){
                    keys[i] = eit.getColumn(keyIndexExcel[i]);
                }
                KeyWrapper key = new KeyWrapper(keys);
                Object[] values = existsRecords.remove(key);
                if (values != null){ // update all columns
                    for (int i=0; i<columnIndexExcel.length; ++i){
                        eit.setColumn(columnIndexExcel[i], values[i]);
                    }
                    if (!ignoreAutoCloseStatusSet.contains(eit.getColumn(statusColumnIndexExcel))){ // force new status
                        if (!newStatusStatus.equals(eit.getColumn(statusColumnIndexExcel))){
                            eit.setColumn(statusColumnIndexExcel, newStatusStatus);
                            newScore++;
                        }
                    }
                } else {
                    if (!ignoreAutoCloseStatusSet.contains(eit.getColumn(statusColumnIndexExcel))){ // force autoclosed status
                        if (!autoCloseStatus.equals(eit.getColumn(statusColumnIndexExcel))){
                            eit.setColumn(statusColumnIndexExcel, autoCloseStatus);
                            autoClosedScore++;
                        }
                    }
                }
                if (!nonOpenStatusSet.contains(eit.getColumn(statusColumnIndexExcel))){ // open status
                    openScore++;
                }
            }
            // new status
            newScore += existsRecords.size();
            openScore += existsRecords.size();
            for (Map.Entry<KeyWrapper, Object[]> e:existsRecords.entrySet()){
                eit.nextRow();
                keys = e.getKey().keys;
                Object[] values = e.getValue();
                for (int i=0; i<keys.length; ++i){
                    eit.setColumn(keyIndexExcel[i], keys[i]);
                }
                for (int i=0; i<columnIndexExcel.length; ++i){
                    eit.setColumn(columnIndexExcel[i], values[i]);
                }
                eit.setColumn(statusColumnIndexExcel, newStatusStatus);
            }
            int[] score = new int[3];
            score[0] = newScore;
            score[1] = autoClosedScore;
            score[2] = openScore;;
            return score;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            IOUtils.closeQuietly(eit);
        }
    }
    
}

