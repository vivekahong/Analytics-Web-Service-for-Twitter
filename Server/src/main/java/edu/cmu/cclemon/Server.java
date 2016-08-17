package edu.cmu.cclemon;

import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.sql.*;
import java.io.*;
import java.util.*;

import java.lang.ClassLoader;
import java.math.BigInteger;
import java.text.SimpleDateFormat;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Team CC Lemon, Final Phase Mixed Undertow Server for MySQL
 */
public class Server {
	protected static final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected static final BigInteger x = new BigInteger(
			"64266330917908644872330635228106713310880186591609208114244758680898150367880703152525200743234420230");

	// Connection
	protected static ComboPooledDataSource cpds;
	protected static Connection conn;

	// Routing Handler
	final static RoutingHandler rootHandler = Handlers.routing();

	// Hash map to store current sequence of the operations
	protected static HashMap<String, Tweet> currentSeq = new HashMap<String, Tweet>();
	protected static HashMap<Integer, String> publicDNS = new HashMap<Integer, String>();

	public static void main(final String[] args) {
		try {
			// Connect to MySQL server
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass("com.mysql.jdbc.Driver");
			cpds.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/project?characterEncoding=utf8");
			cpds.setUser("root");
			cpds.setPassword("lemon");

			// cpds.setMinPoolSize(1);
			// cpds.setAcquireIncrement(5);
			// cpds.setMaxPoolSize(500);
			// cpds.setMaxStatements(10000);

			conn = cpds.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}

		timeFormat.setTimeZone(TimeZone.getTimeZone("EST"));

		publicDNS.put(0, "http://ec2-54-173-137-117.compute-1.amazonaws.com/q4f?");
		publicDNS.put(1, "http://ec2-54-209-98-197.compute-1.amazonaws.com/q4f?");
        publicDNS.put(2, "http://ec2-54-173-134-38.compute-1.amazonaws.com/q4f?");
        publicDNS.put(3, "http://ec2-54-173-228-98.compute-1.amazonaws.com/q4f?");
        publicDNS.put(4, "http://ec2-52-91-7-93.compute-1.amazonaws.com/q4f?");
        publicDNS.put(5, "http://ec2-54-208-155-52.compute-1.amazonaws.com/q4f?");

		rootHandler.add(new HttpString("GET"), "/q1", new Q1Handler())
				.add(new HttpString("GET"), "/q2", new Q2Handler()).add(new HttpString("GET"), "/q3", new Q3Handler())
				.add(new HttpString("GET"), "/q4", new Q4Handler())
				.add(new HttpString("GET"), "/q4f", new Q4fHandler());

		Undertow server = Undertow.builder().addListener(80, "0.0.0.0").setIoThreads(4).setWorkerThreads(200)
				.setHandler(rootHandler).build();
		server.start();
	}
}