package cs425;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private Util() {
    }

    public static String getHostnameFromIp(String ip) throws UnknownHostException {
        return InetAddress.getByName(ip).getHostName();
    }

    public static String getIpFromHostname(String host) throws UnknownHostException {
        return InetAddress.getByName(host).getHostAddress();
    }

    public static String getCurrentHostname() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    /**
     * Thread.sleep without throwing exception
     */
    public static boolean noExceptionSleep(long millSecond) {
        try {
            Thread.sleep(millSecond);
        } catch (InterruptedException e) {
            StackTraceElement[] ste = e.getStackTrace();
            if (ste.length > 1)
                logger.error("Thread sleep failed: <{}:{}>", ste[1].getFileName(), ste[1].getLineNumber());
            return false;
        }
        return true;
    }

    public static String generateUuid() {
        return UUID.randomUUID().toString();
    }

    public static String ungzip(byte[] bytes) throws Exception {
        InputStreamReader isr = new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(bytes)), StandardCharsets.UTF_8);
        StringWriter sw = new StringWriter();
        char[] chars = new char[1024];
        for (int len; (len = isr.read(chars)) > 0; ) {
            sw.write(chars, 0, len);
        }
        return sw.toString();
    }

    public static byte[] gzip(String s) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        OutputStreamWriter osw = new OutputStreamWriter(gzip, StandardCharsets.UTF_8);
        osw.write(s);
        osw.close();
        return bos.toByteArray();
    }

    /**
     * connection to host
     */
    public static Socket connectToServer(String host, int port) throws IOException {
        Socket s = new Socket();
        // Potential higher performance with SO_KA
        s.setKeepAlive(true);
        s.connect(new InetSocketAddress(host, port), Config.CONNECT_TIMEOUT_SECOND * 1000);
        s.setSoTimeout(Config.RW_TIMEOUT_SECOND * 1000);
        // logger.info("Connected to server {}", host);
        return s;
    }

}
