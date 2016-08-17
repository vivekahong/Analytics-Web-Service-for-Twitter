package edu.cmu.cclemon;

import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.math.BigInteger;
import java.util.Map;
import java.util.Deque;

/**
 * Team CC Lemon, Final Phase Query 1 Handler
 */
public class Q1Handler implements HttpHandler {
	private String readSpiral(String message, BigInteger y, String timeStamp) {
		// z is the greatest common divisor (GCD) of x and large message key y
		BigInteger z = Server.x.gcd(y);
		// Minikey K = 1 + Z % 25
		int minikey = z.mod(new BigInteger("25")).intValue() + 1;
		
		int length = message.length();
		int size = (int) Math.sqrt(length);
		char[][] matrix = new char[size][size];
		
		// Convert message to matrix
		for (int i = 0; i < size; i++) {
			for (int j = 0; j < size; j++) {
				matrix[i][j] = message.charAt(i * size + j);
			}
		}

		int left = 0;
		int right = size - 1;
		int up = 0;
		int down = size - 1;
		int i = 0;
		int j = 0;
		char direction = 'r';
		
		StringBuilder sb = new StringBuilder();
		for (int counter = 0; counter < length; counter++) {
			sb.append(matrix[i][j]);
			if (direction == 'r') {
				if (j + 1 == right) {
					direction = 'd';
					up++;
				}
				j++;
			} else if (direction == 'd') {
				if (i + 1 == down) {
					direction = 'l';
					right--;
				}
				i++;
			} else if (direction == 'l') {
				if (j - 1 == left) {
					direction = 'u';
					down--;
				}
				j--;
			} else if (direction == 'u') {
				if (i - 1 == up) {
					direction = 'r';
					left++;
				}
				i--;
			}
		}
		String s = sb.toString();
		
		sb.setLength(0);
		sb.append("C.C.Lemon,134846408459\n").append(timeStamp).append("\n");
		for (int k = 0; k < length; k++) {
			int c = (byte) s.charAt(k);
			if (c - minikey >= 65) {
				sb.append((char)(c - minikey));
			} else {
				sb.append((char)(c + 26 - minikey));
			}
		}
		sb.append("\n");
		
		return sb.toString();
	}

	@Override
	public void handleRequest(final HttpServerExchange exchange) throws Exception {
		if (exchange.isInIoThread()) {
			exchange.dispatch(this);
			return;
		}
		
		String timeStamp = Server.timeFormat.format(new java.util.Date());
		
		// Get parameters
		Map<String,Deque<String>> map = exchange.getQueryParameters();
		BigInteger y = new BigInteger(map.get("key").pop());
		String msg = map.get("message").pop();
		
		// Construct Response
		exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
		exchange.getResponseSender().send(readSpiral(msg, y, timeStamp));
	}
}