package edu.cmu.cclemon;

import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.sql.*;
import java.util.Map;
import java.util.Deque;

/**
 * Team CC Lemon, Final Phase Query 3 Handler
 */
public class Q3Handler implements HttpHandler {
	private String performQuery(String start_date, String end_date, String start_userid, String end_userid, String[] wordArr) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int count1 = 0;
		int count2 = 0;
		int count3 = 0;
		
		String[] a;
		String[] b;
		String[] c;
		
		try {
			String query = "SELECT * FROM wordcount WHERE date BETWEEN " + start_date + " AND " + end_date + " AND user_id BETWEEN " + start_userid + " AND " + end_userid;
			stmt = Server.conn.prepareStatement(query);
			rs = stmt.executeQuery();
			
			String search1 = "," + wordArr[0] + ":";
			String search2 = "," + wordArr[1] + ":";
			String search3 = "," + wordArr[2] + ":";
			
			String word;
			while (rs.next()) {
				word = rs.getString("word");
				
				a = word.split(search1);
				b = word.split(search2);
				c = word.split(search3);
				
				if(a.length == 2) {
					count1 += Integer.parseInt(a[1].split(",")[0]);
				}
				if(b.length == 2) {
					count2 += Integer.parseInt(b[1].split(",")[0]);
				}
				if(c.length == 2) {
					count3 += Integer.parseInt(c[1].split(",")[0]);
				}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			return "C.C.Lemon,134846408459\n" + wordArr[0] + ":" + count1 + "\n" + wordArr[1] + ":" + count2 + "\n" + wordArr[2] + ":" + count3 + "\n";
		}
	}

	@Override
	public void handleRequest(final HttpServerExchange exchange) throws Exception {
		if (exchange.isInIoThread()) {
			exchange.dispatch(this);
			return;
		}
		
		// Get parameters
		Map<String,Deque<String>> map = exchange.getQueryParameters();
		String a = map.get("start_date").pop();
		String b = map.get("end_date").pop();
		String start_date = a.substring(0, 4) + a.substring(5, 7) + a.substring(8);
		String end_date = b.substring(0, 4) + b.substring(5, 7) + b.substring(8);
		String start_userid = map.get("start_userid").pop();
		String end_userid = map.get("end_userid").pop();
		String words = map.get("words").pop();
		String[] wordArr = words.split(",");
		
		// Construct Response
		exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
		exchange.getResponseSender().send(performQuery(start_date, end_date, start_userid, end_userid, wordArr));
	}
}