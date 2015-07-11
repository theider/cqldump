package cqldump;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author theider
 */
public class KeyspaceImport extends HttpServlet {

    private static final Logger log = Logger.getLogger(KeyspaceImport.class);

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");

        DiskFileItemFactory factory = new DiskFileItemFactory();

        ServletContext servletContext = this.getServletConfig().getServletContext();
        File repository = (File) servletContext.getAttribute("javax.servlet.context.tempdir");
        factory.setRepository(repository);

        ServletFileUpload upload = new ServletFileUpload(factory);
        String hostName = null;
        String portText = null;
        String keyspaceName = null;
        String replicationFactorText = null;
        try {
            // Parse the request
            List<FileItem> items = upload.parseRequest(request);
            for (FileItem item : items) {
                log.debug("item:" + item + " size=" + item.getSize());
                if (item.isFormField()) {
                    switch (item.getFieldName()) {
                        case "host":
                            hostName = item.getString();
                            log.debug("host:" + hostName);
                            break;
                        case "port":
                            portText = item.getString();
                            log.debug("port:" + portText);
                            break;
                        case "keyspace":
                            keyspaceName = item.getString();
                            log.debug("keyspace:" + keyspaceName);
                            break;
                        case "replication":
                            replicationFactorText = item.getString();
                            log.debug("replication:" + replicationFactorText);
                            break;
                    }
                }
            }
            if (hostName == null) {
                throw new ServletException("missing required host parameter");
            }
            if (keyspaceName == null) {
                throw new ServletException("missing required keyspaceName parameter");
            }
            if ((portText == null) || portText.isEmpty()) {
                portText = "9042";
            }
            int portNumber = Integer.parseInt(portText);
            if ((replicationFactorText == null) || replicationFactorText.isEmpty()) {
                replicationFactorText = "1";
            }
            int replicationFactor = Integer.parseInt(replicationFactorText);

            for (FileItem item : items) {
                log.debug("item:" + item + " size=" + item.getSize());
                String itemName = item.getName();
                if ((itemName != null) && itemName.endsWith(".zip")) {
                    log.debug("found zip item " + itemName);
                    importKeyspace(item.getInputStream(), hostName, portNumber, keyspaceName, replicationFactor);
                }
            }
        } catch (Exception ex) {
            log.error(ex, ex);
        }

        try (PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Import</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("Keyspace import complete");
            out.println("</body>");
            out.println("</html>");
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

    private void importKeyspace(InputStream inputStream, String hostName, int portNumber, String keyspaceName, int replicationFactor) throws IOException {
        ZipInputStream zin = new ZipInputStream(inputStream);
        // find metadata entry
        Cluster cluster;
        ZipEntry zipEntry = zin.getNextEntry();
        if (zipEntry != null) {
            String entryName = zipEntry.getName();
            log.debug("processing entry " + entryName);
            if (!entryName.endsWith("metadata.json")) {
                throw new IOException("expecting first entry to be METADATA.JSON");
            }
            // process metadata
            cluster = getCluster(hostName, portNumber, keyspaceName, replicationFactor);
            // read into JSON block
            Map<String, TableMetadata> tableMetadata = loadMetadata(zin, cluster, keyspaceName);
            // process entries
            int t = 0;
            do {
                zipEntry = zin.getNextEntry();
                if (zipEntry != null) {
                    entryName = zipEntry.getName();
                    log.debug("processing entry " + entryName);                    
                    loadTableData(zin, cluster, keyspaceName, tableMetadata);
                    t++;
                }
            } while (zipEntry != null);
            log.info(" - completed import of keyspace " + keyspaceName + " imported " + t + " tables.");
        }
    }

    private Cluster getCluster(String hostName, int portNumber, String keyspaceName, int replicationFactor) {
        // connect to cluster
        log.debug("loading keyspace metadata keyspace=" + keyspaceName);
        Cluster cluster = Cluster.builder()
                //.addContactPoint(config.getCassandraHostAddress())
                .addContactPoint(hostName)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                // port
                .withPort(portNumber)
                .build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(60000);
        Metadata metadata = cluster.getMetadata();
        log.info("Connected to Cassandra cluster: " + metadata.getClusterName());
        KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
        if (keyspace == null) {
            // create default keyspace
            try (Session csession = cluster.connect()) {
                // create keyspace
                String keyspaceCreate = "CREATE KEYSPACE " + keyspaceName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" + Integer.toString(replicationFactor) + "};";
                csession.execute(keyspaceCreate);
                log.debug("created keyspace " + keyspaceName);
            }
        }
        return cluster;
    }

    protected class TableMetadata {
        private final String tableName;
        private final Map<String, String> columns = new HashMap<>();

        public TableMetadata(String tableName) {
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
        }

        public Map<String, String> getColumns() {
            return columns;
        }

    }

    private Map<String, TableMetadata> loadMetadata(ZipInputStream zin, Cluster cluster, String keyspaceName) throws IOException {
        log.info("importing metadata keyspace " + keyspaceName);
        Map<String, TableMetadata> typeMap = new HashMap<>();
        String sourceJson = getJSONData(zin);
        JSONObject source = (JSONObject) JSONValue.parse(sourceJson);
        String originalKeyspace = (String) source.get("keyspace");
        log.debug("original keyspace name " + originalKeyspace);

        JSONArray tables = (JSONArray) source.get("tables");
        for (Iterator tableIter = tables.iterator(); tableIter.hasNext();) {
            JSONObject table = (JSONObject) tableIter.next();
            String tableName = (String) table.get("name");
            String createText = (String) table.get("create");
            TableMetadata metadata = new TableMetadata(tableName);
            typeMap.put(tableName, metadata);
            // search and replace new keyspace name
            createText = createText.replaceAll(originalKeyspace, keyspaceName);
            log.info("create table " + tableName + " create:" + createText);
            try (Session csession = cluster.connect()) {
                // create keyspace
                csession.execute(createText);
                // for each table create the indices
                JSONArray columns = (JSONArray) table.get("columns");
                if (columns != null) {
                    for (Iterator it = columns.iterator(); it.hasNext();) {
                        JSONObject column = (JSONObject) it.next();
                        String name = (String) column.get("name");
                        String colType = (String) column.get("type");
                        metadata.getColumns().put(name, colType);
                        String createIndexText = (String) column.get("create_index");
                        if (createIndexText != null) {
                            createIndexText = createIndexText.replaceAll(originalKeyspace, keyspaceName);
                            log.debug(" -- create index " + createIndexText);
                            csession.execute(createIndexText);
                        }
                    }
                }
            }
        }
        return typeMap;
    }

    private static final int BUFFER_SIZE = 32768;

    private byte[] getStreamBytes(InputStream in, int byteCount) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead = 0;
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int r;
        do {
            r = BUFFER_SIZE;
            if (r > byteCount) {
                r = byteCount;
            }
            int c = in.read(buffer, 0, r);
            if (c > 0) {
                bout.write(buffer, 0, c);
                byteCount -= c;
                bytesRead += c;
            } else if(bytesRead != 0) {
                throw new IOException("incomplete stream read expected " + r + " but got " + c + " bytes");
            } else {
                return null;
            }
        } while (byteCount > 0);
        return bout.toByteArray();
    }

    private String getJSONRowData(ZipInputStream zin) throws IOException {
        byte[] newline = getStreamBytes(zin, 2);
        if ((newline != null) && (newline.length == 2)) {
            byte[] hexBuf = getStreamBytes(zin, 8);
            if (hexBuf.length == 8) {
                String hexText = new String(hexBuf);
                log.debug("[" + hexText + "]");
                hexText = hexText.trim();
                int dataLength = Integer.parseInt(hexText, 16);
                log.debug("record len:" + dataLength);
                byte[] data = getStreamBytes(zin, dataLength);
                return new String(data);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private String getJSONData(ZipInputStream zin) throws IOException {
        int r;
        byte[] buffer = new byte[BUFFER_SIZE];
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        do {
            r = zin.read(buffer, 0, BUFFER_SIZE);
            if (r > 0) {
                bout.write(buffer, 0, r);
            }
        } while (r > 0);
        String jsonData = new String(bout.toByteArray());
        return jsonData;
    }

    private void loadTableData(ZipInputStream zin, Cluster cluster, String keyspaceName, Map<String, TableMetadata> tableMetadata) throws IOException {
        // table data is a series of records.
        // 8 bytes of text representing a hex length
        // that length is how long the JSON is for the record data.
        // skip the first two chars
        try (Session csession = cluster.connect(keyspaceName)) {
            String tableData;
            int c = 0;
            do {
                tableData = getJSONRowData(zin);
                if (tableData != null) {                    
                    log.debug(c + " row data:" + tableData);                    
                    JSONObject record = (JSONObject) JSONValue.parse(tableData);
                    String tableName = (String) record.get("table_name");
                    if(c == 0) {
                        log.info("importing table data " + keyspaceName + ":" + tableName);
                    } else if((c % 1000) == 0) {
                        log.info(" ... imported " + c + " rows");
                    }
                    
                    TableMetadata metadata = tableMetadata.get(tableName);
                    JSONObject rowData = (JSONObject) record.get("data");                    
                    log.debug("record: " + record);
                    // series of values
                    StringBuilder sb = new StringBuilder();
                    sb.append("INSERT INTO ");
                    sb.append(tableName);
                    sb.append(" (");
                    int i=0;
                    for (Iterator it = rowData.keySet().iterator(); it.hasNext();) {
                        String colName = (String) it.next();
                        if(i != 0) {
                            sb.append(',');
                        }
                        sb.append(colName);
                        i++;
                    }
                    int columnCount = rowData.size();
                    sb.append(") VALUES (");
                    for(i=0; i < columnCount;i++) {
                        if(i != 0) {
                            sb.append(',');
                        }
                        sb.append('?');
                    }
                    sb.append(");");
                    PreparedStatement prepStmt = csession.prepare(sb.toString());
                    List<Object> columnData = new ArrayList<>();
                    i = 0;
                    for (Iterator it = rowData.keySet().iterator(); it.hasNext();) {
                        String colName = (String) it.next();
                        String dataValue = (String) rowData.get(colName);
                        if(dataValue == null) {
                            columnData.add(null);
                        } else {
                            String colType = metadata.getColumns().get(colName);
                            switch(colType) {
                                case "timestamp":
                                    SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                                    Date d = fmt.parse(dataValue);
                                    columnData.add(d);
                                    break;
                                case "blob":
                                    byte[] blobData = Base64.decodeBase64(dataValue);
                                    ByteBuffer buffer = ByteBuffer.wrap(blobData);
                                    columnData.add(buffer);
                                    break;
                                case "double":
                                    Double dv = Double.parseDouble(dataValue);
                                    columnData.add(dv);
                                    break;
                                case "bigint":
                                    Long ln = Long.parseLong(dataValue);
                                    columnData.add(ln);
                                    break;
                                case "boolean":
                                    Boolean bv = Boolean.parseBoolean(dataValue);
                                    columnData.add(bv);
                                    break;
                                case "int":
                                    Integer n = Integer.parseInt(dataValue);
                                    columnData.add(n);
                                    break;
                                case "timeuuid":
                                case "uuid":
                                    columnData.add(UUID.fromString(dataValue));
                                    break;
                                default:
                                    columnData.add(dataValue);
                                    break;
                            }
                        }
                        i++;
                    }                                                            
                    BoundStatement bprep = new BoundStatement(prepStmt);
                    Object[] dataValues = columnData.toArray();
                    bprep.bind(dataValues);
                    csession.execute(bprep);
                    c++;
                }
            } while (tableData != null);
            log.info(" -- table import complete imported " + c + " rows");
        } catch (ParseException ex) {
            throw new IOException("failed to load data",ex);
        }
    }
}

