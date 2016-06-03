package com.cloudera.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.datanucleus.store.rdbms.query.AbstractRDBMSQueryResult;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.client.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

public class KuduDataServlet extends HttpServlet {
  private static final String KUDU_MASTER = System.getProperty(
      "kuduMaster", "ip-10-1-1-142.us-west-2.compute.internal");

  private final static String TABLE_NAME = System.getProperty(
      "tableName", "tickdb");

  private final List<String> schema = new ArrayList<String>() {
    {
      add("ts_received");
      add("ticket");
      add("per");
      add("last");
      add("vol");
    }
  };

  //// TODO: 5/28/16 add initialization method to reuse KuduClient connection
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    response.setContentType("application/javascript");
    response.setStatus(HttpServletResponse.SC_OK);
    //Map<String, Map<String, CountListOfStringsPair>> responseData = new HashMap<String, Map<String, CountListOfStringsPair>>();
    // TreeSet<String> set = new TreeSet<String>();
    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
    try {
      KuduTable table = client.openTable(TABLE_NAME);
      KuduSession session = client.newSession();

      // value <= v
      ColumnSchema tsCol = new ColumnSchema.ColumnSchemaBuilder("ts_received", Type.INT64).key(true).build();
      long hourAgo = System.currentTimeMillis() - (1000*60*60);
      System.out.println("hour ago: " + hourAgo);
      KuduPredicate lessEqualHour = KuduPredicate.newComparisonPredicate(tsCol, KuduPredicate.ComparisonOp.GREATER_EQUAL, hourAgo);
      Map<String, List<Long>> responseData = new HashMap<String, List<Long>>();
      KuduScanner scanner = client.newScannerBuilder(table)
              .addPredicate(lessEqualHour)
              .setProjectedColumnNames(schema)
              .build();
      List events = new ArrayList<TickEvent>();
      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult result = results.next();
          Event e = new Event();
          e.ticket = result.getString("ticket");
          e.per = result.getString("per");
          e.last = result.getString("last");
          e.vol = result.getString("vol");
          e.ts = result.getLong("ts_received");
          events.add(e);

        }
      }
      System.out.println(events.size());


    mapper.writer().writeValue(response.getWriter(), events);
    mapper.writeValue(response.getWriter(), events);
  } catch (Exception ex) {
    ex.printStackTrace();
  } finally {
    try {
      client.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  }

  private class Event {
    public String ticket;
    public String per;
    public String last;
    public String vol;
    public Long ts;
  }
}
