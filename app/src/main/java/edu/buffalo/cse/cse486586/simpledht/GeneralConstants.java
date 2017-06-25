package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by opensam on 3/7/17.
 */

public final class GeneralConstants {
    public static final int SERVER_PORT = 10000;
    public static final String ALL_PORTS[] = new String[]{"11108", "11112", "11116", "11120", "11124"};
    public static final String ACK_MSG = "HOUSTEN_I_GOT_YOU";
    public static final int SOCKET_TIMEOUT = 1000;
    public static final int BASE_EMULATOR_ID = 5554;
    public static final String LOCAL = "@";
    public static final String GLOBAL = "*";
    public static final long LOCK_TIMEOUT=1000;
    public static final int LOCAL_ID  = 1 ;
    public static final int GLOBAL_ID = 2 ;
    public static final int DEFAULT_ID = 3 ;
    public static final int ACTION_JOIN = 1;
    public static final int ACTION_UPDATE_SUCC = 2;
    public static final int ACTION_UPDATE_PRED = 3;
    public static final int ACTION_REDISTRIBUTE_MSGS = 4;
    public static final int ACTION_INSERT = 5;
    public static final int ACTION_QUERY = 6;
    public static final int ACTION_NUCLEAR_QUERY = 7;
    public static final int ACTION_DELETE = 8;
    public static final int ACTION_NUCLEAR_DELETE = 9;
    public static final String ACTION = "Action";
    public static final String CLIENT_ID = "Client_Port";
    public static final String KEY = "key";
    public static final String VALUE = "value";
}
