package edu.cmu.cclemon;

import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.*;

/**
 * Team CC Lemon, Final Phase Query 3 Handler
 */
public class Q4Handler implements HttpHandler {

	private String performSetQuery(String tweetid, int seq, String fields, String payload, Tweet current) {
		PreparedStatement stmt = null;
		String[] fieldsList = fields.split(",");
		String[] payloadList = payload.split(",");
		int payloadLen = payloadList.length;
		int fieldLen = fieldsList.length;

		try {
			if(fieldLen != 0) {

				StringBuilder sb = new StringBuilder();
				int length = Math.min(fieldsList.length, payloadList.length);
				if(payloadLen != 0) {
					if (!current.didSet) {
						current.didSet = true;
						// Perform Insert
						sb.append("INSERT INTO phase3 (tweetid");
						for (int i = 0; i < length; i++) {
							sb.append("," + fieldsList[i]);
						}

					
						sb.append(") VALUES (" + tweetid);
						for (int i = 0; i < length; i++) {
							sb.append(",\"" + payloadList[i] + "\"");
						}
						sb.append(")");
					} else {
						// Perform Update
						sb.append("UPDATE phase3 SET " + fieldsList[0] + "=\"" + payloadList[0] + "\"");
						for (int i = 1; i < length; i++) {
							sb.append(", " + fieldsList[i] + "=\"" + payloadList[i] + "\"");
						}
						sb.append(" WHERE tweetid=" + tweetid);
					}
				}else{
					if (!current.didSet) {
						current.didSet = true;
						// Perform Insert
						sb.append("INSERT INTO phase3 (tweetid");
						for (int i = 0; i < length; i++) {
							sb.append("," + fieldsList[i]);
						}

					
						sb.append(") VALUES (" + tweetid);
						for (int i = 0; i < length; i++) {
							sb.append(",\"\"");
						}
						sb.append(")");

					} else {
						// Perform Update
						sb.append("UPDATE phase3 SET " + fieldsList[0] + "=\"\"");
						for (int i = 1; i < length; i++) {
							sb.append(", " + fieldsList[i] + "=\"\"");
						}
						sb.append(" WHERE tweetid=" + tweetid);
					}
				}

				stmt = Server.conn.prepareStatement(sb.toString());
				stmt.executeUpdate();
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
			synchronized (current) {
				current.seq++;
				current.notifyAll();
			}

			return "C.C.Lemon,134846408459\nsuccess\n";
		}
	}

	private String performGetQuery(String tweetid, int seq, String field, Tweet current) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String result = "\n";

		try {
			String query = "SELECT " + field + " FROM phase3 WHERE tweetid =" + tweetid + " LIMIT 1";
			stmt = Server.conn.prepareStatement(query);
			rs = stmt.executeQuery();
			if (rs.next()) {
				String s = rs.getString(1);
				if (s != null) {
					result = s.replaceAll(" ", "+") + "\n";
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

			synchronized (current) {
				current.seq++;
				current.notifyAll();
			}

			return "C.C.Lemon,134846408459\n" + result;
		}
	}

	@Override
	public void handleRequest(final HttpServerExchange exchange) throws Exception {
		if (exchange.isInIoThread()) {
			exchange.dispatch(this);
			return;
		}

		// Get parameters
		Map<String, Deque<String>> map = exchange.getQueryParameters();
		String tweetid = map.get("tweetid").peek();
		int hash = (tweetid.charAt(16)+tweetid.charAt(17))%6;
		
		if (hash != 5) {
			String request=exchange.getQueryString();
			URL url = new URL(Server.publicDNS.get(hash)+request);
			URLConnection conn = url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			StringBuilder sb=new StringBuilder();
			String line;
			while((line=in.readLine())!=null){
				sb.append(line);
				sb.append("\n");
			}
			in.close();
			exchange.getResponseSender().send(sb.toString());

		} else {
			int seq = Integer.parseInt(map.get("seq").peek());

			exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");

			Tweet current = Server.currentSeq.get(tweetid);
			// Check sequence of tweetid
			if (current == null) {
				current = new Tweet(1, false);
				Server.currentSeq.put(tweetid, current);
			}

			while (true) {
				synchronized (current) {
					if (seq != current.seq) {
						try {
							current.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					} else {
						break;
					}
				}
			}

			// Construct Response
			String op = map.get("op").pop();
			String fields = map.get("fields").pop();
			if (op.toLowerCase().equals("set")) {
				String payload = map.get("payload").pop();
				exchange.getResponseSender().send(performSetQuery(tweetid, seq, fields, payload, current));
			} else if (op.toLowerCase().equals("get")) {
				exchange.getResponseSender().send(performGetQuery(tweetid, seq, fields, current));
			}
		}
	}
}