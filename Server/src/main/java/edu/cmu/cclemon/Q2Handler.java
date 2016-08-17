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
 * Team CC Lemon, Final Phase Query 2 Handler
 */
public class Q2Handler implements HttpHandler {
	private String performQuery(String key) {
		String response = "";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String query = "SELECT text FROM tweets WHERE id_hash = '" + key + "' LIMIT 1";
			stmt = Server.conn.prepareStatement(query);
			rs = stmt.executeQuery();
			
			String text = "";
			if(rs.next()) {
				text = rs.getString("text");
			}
				
			if(text.length() == 0) {
				response = "C.C.Lemon,134846408459\n\n";
			} else {
				response = "C.C.Lemon,134846408459\n" + text;
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

			return response;
			
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
		String uid = "";
		String htg = "";
		
		try{
			uid = map.get("userid").pop();
			htg = map.get("hashtag").pop();
		} catch (NullPointerException e) {

		}
		
		// Construct Response
		if(uid != null && htg != null) {
			exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
			exchange.getResponseSender().send(performQuery(uid + htg));
		} else {
			exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
			exchange.getResponseSender().send("C.C.Lemon,134846408459\n\n");
		}
		
	}
}