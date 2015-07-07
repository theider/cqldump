package cqldump;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author theider
 */
public class KeyspaceExport extends HttpServlet {

    private static final Logger log = Logger.getLogger(KeyspaceExport.class);

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
        String hostAddress = request.getParameter("host");
        if(hostAddress == null) {
            throw new ServletException("missing required host parameter");
        }
        String keyspaceName = request.getParameter("keyspace");
        if(keyspaceName == null) {
            throw new ServletException("missing required keyspaceName parameter");
        }
        String portText = request.getParameter("port");
        if( (portText == null) || portText.isEmpty() ) {
            portText = "9042";
        }
        int portNumber = Integer.parseInt(portText);
        Cluster cluster = Cluster.builder()
                //.addContactPoint(config.getCassandraHostAddress())
                .addContactPoint(hostAddress)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                // port
                .withPort(portNumber)
                .build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(60000);
        Metadata metadata = cluster.getMetadata();
        log.info("Connected to Cassandra cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            log.info(String.format("Datatacenter: %s; Host: %s; Rack: %s",
                    host.getDatacenter(), host.getAddress(), host.getRack()));
        }        
        KeyspaceMetadata md = metadata.getKeyspace(keyspaceName);
        if (md == null) {
            throw new IOException("keyspace not found: " + keyspaceName);
        }
        response.setContentType("application/zip");
        JSONObject mdObject = new JSONObject();
        mdObject.put("keyspace", keyspaceName);
        try (ZipOutputStream zout = new ZipOutputStream(response.getOutputStream())) {
            log.info("export data from keyspace " + md.getName());
            JSONArray tablesArray = new JSONArray();
            for (TableMetadata tmd : md.getTables()) {
                log.info("export table " + tmd.getName());
                // make a JSON object for the description
                JSONObject tableObject = new JSONObject();
                tableObject.put("name", tmd.getName());
                tableObject.put("create", tmd.asCQLQuery());
                JSONArray columnsArray = new JSONArray();
                for (ColumnMetadata cmd : tmd.getColumns()) {
                    JSONObject columnObject = new JSONObject();
                    columnObject.put("name", cmd.getName());
                    String colName = cmd.getType().getName().toString();
                    columnObject.put("type", colName);
                    if (cmd.getIndex() != null) {
                        columnObject.put("create_index", cmd.getIndex().asCQLQuery());
                    }
                    columnsArray.add(columnObject);
                }
                tableObject.put("columns", columnsArray);
                tablesArray.add(tableObject);
            }
            mdObject.put("tables", tablesArray);
            String mdText = mdObject.toJSONString();
            ZipEntry ze = new ZipEntry(keyspaceName + "/metadata.json");
            zout.putNextEntry(ze);
            zout.write(mdText.getBytes());
            // next query all records in each table
            for (TableMetadata tmd : md.getTables()) {
                log.info("export table " + tmd.getName());
                try (Session hsession = cluster.connect(keyspaceName)) {
                    ze = new ZipEntry(keyspaceName + "/" + tmd.getName() + ".json");
                    zout.putNextEntry(ze);
                    String tableName = tmd.getName();
                    // content is a series of JSON objects with a 32 bit hex prefix indicating text size of record JSON                    
                    Statement stmt = new SimpleStatement("SELECT * FROM " + tableName);
                    log.info("fetching records from " + tmd.getName());
                    stmt.setFetchSize(1000);
                    ResultSet rs = hsession.execute(stmt);
                    Iterator<Row> iter = rs.iterator();
                    int c = 0;
                    while(iter.hasNext()) {
                        Row row = iter.next();                        
                        JSONObject rowObject = new JSONObject();
                        JSONObject rowData = new JSONObject();
                        for (Definition key : row.getColumnDefinitions().asList()) {
                            Object o = row.getObject(key.getName());
                            if(o == null) {
                                rowData.put(key.getName(), null);
                            } else if(key.getType() == DataType.timeuuid()) {
                                UUID uuid = (UUID) o;
                                String txt = uuid.toString();
                                rowData.put(key.getName(), txt);
                            } else if(key.getType() == DataType.uuid()) {
                                UUID uuid = (UUID) o;
                                String txt = uuid.toString();
                                rowData.put(key.getName(), txt);
                            } else if(key.getType() == DataType.timestamp()) {
                                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
                                String text = fmt.format(row.getTimestamp(key.getName()));
                                rowData.put(key.getName(), text);
                            } else {
                                rowData.put(key.getName(), o.toString());
                            }                            
                        }
                        rowObject.put("data", rowData);
                        rowObject.put("table_name", tableName);
                        byte[] textData = rowObject.toJSONString().getBytes();
                        String sizeHex = String.format("\r\n%8s", Integer.toHexString(textData.length));
                        zout.write(sizeHex.getBytes());
                        zout.write(textData);
                        c++;
                    }
                    log.info(" -- read " + c + " records");
                }
            }

            zout.finish();
            zout.close();

            log.info("done writing table data to output zip");
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

}
