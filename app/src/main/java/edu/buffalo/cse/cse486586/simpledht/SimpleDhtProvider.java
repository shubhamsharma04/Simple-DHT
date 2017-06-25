package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getName();
    private static final String sKey = "key";
    private static final String sValue = "value";
    private static final String[] columns = new String[]{sKey, sValue};
    // This would be populated by MainActivity
    public static String emulatorId = null;
    // This would be populated by MainActivity
    public static String myPort = null;
    // This would be populated by this class
    public static String myID = null;

    private static Uri mUri;

    private static Context context;

    private SimpleDhtContentHelper simpleDhtContentHelper;

    public static boolean[] isAlive;

    public static String prev;
    public static String next;

    public static String redisMsgs = "";

    public static StringBuilder response;

    public static List<String> hashedNodes;

    public static Map<String, String> nodeToHashMap;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v(TAG, "Delete for key : " + selection);
        String key = selection;
        int action = 0;
        if (GeneralConstants.GLOBAL.equals(key)) {
            if (SimpleDhtProvider.myPort.equals(SimpleDhtProvider.next)) {
                action = GeneralConstants.LOCAL_ID;
            } else {
                action = GeneralConstants.GLOBAL_ID;
            }

        } else if (GeneralConstants.LOCAL.equals(key)) {
            action = GeneralConstants.LOCAL_ID;
        } else {
            action = GeneralConstants.DEFAULT_ID;
        }
        MatrixCursor cursor = new MatrixCursor(columns);
        switch (action) {
            case GeneralConstants.DEFAULT_ID:

                // In case there is just 1 node - the base node
                if (SimpleDhtProvider.myPort.equals(SimpleDhtProvider.next)) {
                    Log.v(TAG, "Only 1 avd up");
                    deleteFromCurrentNode(key, action);

                } else {
// In case there are more than 1 node
                    try {
                        String hashedKey = genHash(key);
                        String prevHash = genHash(String.valueOf(Integer.parseInt(SimpleDhtProvider.prev) / 2));
                        String currHash = (SimpleDhtProvider.myID);
                        String nextHash = genHash(String.valueOf(Integer.parseInt(SimpleDhtProvider.next) / 2));
                        //Log.i(TAG, "Curr Emu id1 : " + SimpleDhtProvider.emulatorId + " Next EmuId : " + SimpleDhtProvider.next + " Prev EmuId : " + SimpleDhtProvider.prev);
                        Log.i(TAG, "Prev : " + SimpleDhtProvider.prev + " mine : " + myPort + " Next : " + SimpleDhtProvider.next);
                        Log.i(TAG, "hashedKey : " + hashedKey + " prevHash : " + prevHash + " currHash : " + currHash + " nextHash : " + nextHash);
                        // The edge case of n-1 and 1
                        if (prevHash.compareTo(currHash) > 0) {
                            if (prevHash.compareTo(hashedKey) < 0 || currHash.compareTo(hashedKey) >= 0) {
                                // Store it
                                deleteFromCurrentNode(key, action);
                            } else {
                                // Call the next guy
                                Log.i(TAG, "In base avd Sending hashedKey : " + hashedKey + " to hashedNode : " + nextHash);
                                deleteTheBuck(key);
                            }
                        } else {
                            if (prevHash.compareTo(hashedKey) < 0 && currHash.compareTo(hashedKey) >= 0) {
                                // Store it
                                deleteFromCurrentNode(key, action);
                            } else {
                                // Call the next guy
                                deleteTheBuck(key);
                            }

                        }
                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "", e);
                    }
                }
                break;
            case GeneralConstants.LOCAL_ID:
                Log.v(TAG, "Delete all the data from current");
                deleteFromCurrentNode(key, action);
                break;
            case GeneralConstants.GLOBAL_ID:
                // Contact BaseEmulator if Required
                Log.v(TAG, "Received delete request for all data stuff");
                if (!SimpleDhtProvider.myPort.equals(String.valueOf(GeneralConstants.BASE_EMULATOR_ID * 2))) {
                    deleteEntireDataSet(key);
                } else {
                    processNuclearDelete();
                }
                break;
        }
        return 0;
    }

    private void processNuclearDelete() {
        Log.v(TAG, "Process Nuclear Delete");
        deleteFromCurrentNode(GeneralConstants.LOCAL, GeneralConstants.LOCAL_ID);
        // Obtain data from all active nodes
        int length = SimpleDhtProvider.isAlive.length;
        for (int i = 0; i < length; i++) {
            if (SimpleDhtProvider.isAlive[i]) {
                String remotePort = SimpleDhtProvider.nodeToHashMap.get(SimpleDhtProvider.hashedNodes.get(i));
                Log.v(TAG, "Emptying remotePort : " + remotePort);
                if (!remotePort.equals(String.valueOf(GeneralConstants.BASE_EMULATOR_ID * 2))) {
                    deleteTheBuck(GeneralConstants.LOCAL);
                    Log.v(TAG, "remotePort : " + remotePort + " emptied");
                }
            }
        }
        Log.v(TAG, "Done with processNuclearQuery");
    }

    private void deleteEntireDataSet(String key) {
        Log.v(TAG, "Inside getEntireDataSet ");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(GeneralConstants.ACTION_NUCLEAR_DELETE), SimpleDhtProvider.myPort, String.valueOf(GeneralConstants.BASE_EMULATOR_ID * 2), key);
        Log.v(TAG, "Done with deleteEntireDataSet");
    }

    private void deleteTheBuck(String key) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(GeneralConstants.ACTION_DELETE), SimpleDhtProvider.myPort, SimpleDhtProvider.next, key, "");
    }

    private void deleteFromCurrentNode(String key, int action) {
        Log.v(TAG, "deleteFromCurrentNode key :  " + key + " action : " + action);
        simpleDhtContentHelper.deleteContentForKey(key, action);
        Log.v(TAG, "deleteFromCurrentNode key : " + key + " done");
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v("insert", values.toString());
        String key = values.getAsString(sKey);
        Log.i(TAG, "Received insert request for key : " + key);
        String hashedKey = null;
        String value = values.getAsString(sValue);
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "", e);
            return null;
        }

        // In case there is just 1 node - the base node
        if (SimpleDhtProvider.myPort.equals(SimpleDhtProvider.next)) {
            uri = storeInCurrentNode(key, value, uri);

        } else {
// In case there are more than 1 node
            try {
                String prevHash = genHash(String.valueOf(Integer.parseInt(SimpleDhtProvider.prev) / 2));
                String currHash = SimpleDhtProvider.myID;
                String nextHash = genHash(String.valueOf(Integer.parseInt(SimpleDhtProvider.next) / 2));
                //Log.i(TAG, "Curr Emu id : " + SimpleDhtProvider.emulatorId + " Next EmuId : " + SimpleDhtProvider.next + " Prev EmuId : " + SimpleDhtProvider.prev);
                Log.i(TAG, "Prev : " + SimpleDhtProvider.prev + " mine : " + myPort + " Next : " + SimpleDhtProvider.next);
                Log.i(TAG, "hashedKey : " + hashedKey + " prevHash : " + prevHash + " currHash : " + currHash + " nextHash : " + nextHash);
                // The edge case of n-1 and 1
                if (prevHash.compareTo(currHash) > 0) {
                    if (prevHash.compareTo(hashedKey) < 0 || currHash.compareTo(hashedKey) >= 0) {
                        // Store it
                        uri = storeInCurrentNode(key, value, uri);
                    } else {
                        // Call the next guy
                        Log.i(TAG, "In base avd Sending hashedKey : " + hashedKey + " to hashedNode : " + nextHash);
                        passTheBuck(key, value);
                    }
                } else {
                    if (prevHash.compareTo(hashedKey) < 0 && currHash.compareTo(hashedKey) >= 0) {
                        // Store it
                        uri = storeInCurrentNode(key, value, uri);
                    } else {
                        // Call the next guy
                        Log.i(TAG, "Sending hashedKey : " + hashedKey + " to hashedNode : " + nextHash);
                        passTheBuck(key, value);
                    }

                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "", e);
            }
        }


        return uri;
    }

    private void passTheBuck(String key, String value) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(GeneralConstants.ACTION_INSERT), SimpleDhtProvider.myPort, SimpleDhtProvider.next, key, value);
    }

    private Uri storeInCurrentNode(String key, String value, Uri uri) {
        Log.v(TAG, "Inserting key : " + key + " with value : " + value);
        if (!simpleDhtContentHelper.writeContent(key, value)) {
            uri = null;
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        context = getContext();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");


        // Populated hashedKeys
        hashedNodes = populateHashedNodes();
        isAlive = new boolean[GeneralConstants.ALL_PORTS.length];
        //isAlive[0] = true;
        //isAlive[isAlive.length - 1] = true;
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        emulatorId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(emulatorId) * 2));
        try {
            myID = genHash(emulatorId);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "", e);
        }
        simpleDhtContentHelper = new SimpleDhtContentHelper();
        boolean result = true;
        try {
            ServerSocket serverSocket = new ServerSocket(GeneralConstants.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            result = false;
        }
        if (result) {
            // If not base avd, then announce your arrival on the scene
            if (!String.valueOf(GeneralConstants.BASE_EMULATOR_ID).equals(emulatorId)) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(GeneralConstants.ACTION_JOIN), SimpleDhtProvider.myPort);
            } else { // This is the base emulator
                prev = myPort;
                next = myPort;
            }
        }
        return result;
    }

    private List<String> populateHashedNodes() {
        List<String> list = new ArrayList<String>();
        nodeToHashMap = new HashMap<String, String>();
        int length = GeneralConstants.ALL_PORTS.length;
        for (int i = 0; i < length; i++) {
            try {
                String hashedValue = genHash(String.valueOf(Integer.valueOf(GeneralConstants.ALL_PORTS[i]) / 2));
                list.add(hashedValue);
                nodeToHashMap.put(hashedValue, GeneralConstants.ALL_PORTS[i]);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "", e);
            }
        }
        Collections.sort(list);
        //list.add(list.get(0));
        for (int i = 0; i < length; i++) {
            Log.v(TAG, i + "th node : " + list.get(i));
        }
        for (Map.Entry<String, String> entry : nodeToHashMap.entrySet()) {
            Log.v(TAG, "key : " + entry.getKey() + "  Value : " + entry.getValue());
        }
        return list;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.v(TAG, "Query for key : " + selection);
        String key = selection;
        int action = 0;
        if (GeneralConstants.GLOBAL.equals(key)) {
            if (SimpleDhtProvider.myPort.equals(SimpleDhtProvider.next)) {
                action = GeneralConstants.LOCAL_ID;
            } else {
                action = GeneralConstants.GLOBAL_ID;
            }

        } else if (GeneralConstants.LOCAL.equals(key)) {
            action = GeneralConstants.LOCAL_ID;
        } else {
            action = GeneralConstants.DEFAULT_ID;
        }
        MatrixCursor cursor = new MatrixCursor(columns);
        switch (action) {
            case GeneralConstants.DEFAULT_ID:

                // In case there is just 1 node - the base node
                if (SimpleDhtProvider.myPort.equals(SimpleDhtProvider.next)) {
                    Log.v(TAG, "Only 1 avd up");
                    cursor = (MatrixCursor) getFromCurrentNode(key, action);

                } else {
// In case there are more than 1 node
                    try {
                        String hashedKey = genHash(key);
                        String prevHash = genHash(String.valueOf(Integer.parseInt(SimpleDhtProvider.prev) / 2));
                        String currHash = (SimpleDhtProvider.myID);
                        String nextHash = genHash(String.valueOf(Integer.parseInt(SimpleDhtProvider.next) / 2));
                        //Log.i(TAG, "Curr Emu id1 : " + SimpleDhtProvider.emulatorId + " Next EmuId : " + SimpleDhtProvider.next + " Prev EmuId : " + SimpleDhtProvider.prev);
                        Log.i(TAG, "Prev : " + SimpleDhtProvider.prev + " mine : " + myPort + " Next : " + SimpleDhtProvider.next);
                        Log.i(TAG, "hashedKey : " + hashedKey + " prevHash : " + prevHash + " currHash : " + currHash + " nextHash : " + nextHash);
                        // The edge case of n-1 and 1
                        if (prevHash.compareTo(currHash) > 0) {
                            if (prevHash.compareTo(hashedKey) < 0 || currHash.compareTo(hashedKey) >= 0) {
                                // Store it
                                cursor = (MatrixCursor) getFromCurrentNode(key, action);
                            } else {
                                // Call the next guy
                                Log.i(TAG, "In base avd Sending hashedKey : " + hashedKey + " to hashedNode : " + nextHash);
                                cursor = (MatrixCursor) checkTheBuck(key);
                            }
                        } else {
                            if (prevHash.compareTo(hashedKey) < 0 && currHash.compareTo(hashedKey) >= 0) {
                                // Store it
                                cursor = (MatrixCursor) getFromCurrentNode(key, action);
                            } else {
                                // Call the next guy
                                Log.i(TAG, "Sending hashedKey : " + hashedKey + " to hashedNode : " + nextHash);
                                cursor = (MatrixCursor) checkTheBuck(key);
                            }

                        }
                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "", e);
                    }
                }
                break;
            case GeneralConstants.LOCAL_ID:
                Log.v(TAG, "Get all the data from current");
                cursor = (MatrixCursor) getFromCurrentNode(key, action);
                break;
            case GeneralConstants.GLOBAL_ID:
                // Contact BaseEmulator if Required
                Log.v(TAG, "Received request for all data stuff");
                if (!SimpleDhtProvider.myPort.equals(String.valueOf(GeneralConstants.BASE_EMULATOR_ID * 2))) {
                    cursor = (MatrixCursor) getEntireDataSet(key);
                } else {
                    cursor = (MatrixCursor) processNuclearQuery();
                }
                break;
        }
        return cursor;
    }

    private Cursor processNuclearQuery() {
        Log.v(TAG, "Process Nuclear Query");
        MatrixCursor cursor = new MatrixCursor(columns);
        // Obtain data from current node
        cursor = (MatrixCursor) getFromCurrentNode(GeneralConstants.LOCAL, GeneralConstants.LOCAL_ID);
        // Obtain data from all active nodes
        int length = SimpleDhtProvider.isAlive.length;
        for (int i = 0; i < length; i++) {
            if (SimpleDhtProvider.isAlive[i]) {
                String remotePort = SimpleDhtProvider.nodeToHashMap.get(SimpleDhtProvider.hashedNodes.get(i));
                Log.v(TAG, "Querying remotePort : " + remotePort);
                if (!remotePort.equals(String.valueOf(GeneralConstants.BASE_EMULATOR_ID * 2))) {
                    cursor = (MatrixCursor) checkTheBuck(GeneralConstants.LOCAL, cursor, remotePort);
                    Log.v(TAG, "remotePort : " + remotePort + " returned cursor with : " + cursor.getCount());
                }
            }
        }
        Log.v(TAG, "Done with processNuclearQuery. Returning cursor with count : " + cursor.getCount());
        return cursor;
    }

    private Cursor checkTheBuck(String key, MatrixCursor cursor, String remotePort) {
        Log.v(TAG, "Asking for dataSet from port : " + remotePort);
        new ClientTask().doInBackground(String.valueOf(GeneralConstants.ACTION_QUERY), SimpleDhtProvider.myPort, remotePort, key);
        String[] response = SimpleDhtProvider.response.toString().split(",");
        Log.v(TAG, "Response received : " + SimpleDhtProvider.response.toString());
        int length = response.length;
        if (length > 1) {
            for (int i = 0; i < length; i = i + 2) {
                Object[] row = new Object[]{response[i], response[i + 1]};
                cursor.addRow(row);
            }
        }
        Log.v(TAG, "Done with dataSet from port : " + remotePort);
        return cursor;
    }

    private Cursor getEntireDataSet(String key) {
        Log.v(TAG, "Inside getEntireDataSet ");
        new ClientTask().doInBackground(String.valueOf(GeneralConstants.ACTION_NUCLEAR_QUERY), SimpleDhtProvider.myPort, String.valueOf(GeneralConstants.BASE_EMULATOR_ID * 2), key);
        MatrixCursor cursor = new MatrixCursor(columns);
        String[] response = SimpleDhtProvider.response.toString().split(",");
        int length = response.length;
        if (length > 1) {
            for (int i = 0; i < length; i = i + 2) {
                Object[] row = new Object[]{response[i], response[i + 1]};
                cursor.addRow(row);
            }
        }
        Log.v(TAG, "Done with getEntireDataSet");
        return cursor;
    }

    // Does not handle the case where key is not present at all
    private Cursor checkTheBuck(String key) {
        new ClientTask().doInBackground(String.valueOf(GeneralConstants.ACTION_QUERY), SimpleDhtProvider.myPort, SimpleDhtProvider.next, key);
        MatrixCursor cursor = new MatrixCursor(columns);
        String[] response = SimpleDhtProvider.response.toString().split(",");
        int length = response.length;
        if (length > 1) {
            for (int i = 0; i < length; i = i + 2) {
                Object[] row = new Object[]{response[i], response[i + 1]};
                cursor.addRow(row);
            }
        }
        return cursor;
    }

    private Cursor getFromCurrentNode(String key, int action) {
        Log.v(TAG, "getFromCurrentNode key :  " + key + " action : " + action);
        MatrixCursor cursor = new MatrixCursor(columns);
        Map<String, String> map = simpleDhtContentHelper.getContentForKey(key, action);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Object[] row = new Object[]{entry.getKey(), entry.getValue()};
            cursor.addRow(row);
        }
        Log.v(TAG, "Returning Cursor : " + cursor.toString() + " with count : " + cursor.getCount());
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        Log.v(TAG, "Update for key : " + selection);
        if (selectionArgs == null) {
            String key = null;
            try {
                key = genHash(String.valueOf(Integer.valueOf(selection) / 2));
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "", e);
                return -1;
            }

            key = simpleDhtContentHelper.updateContentForKey(key);
            SimpleDhtProvider.redisMsgs = key;
        } else {
            // Batch Insert
            simpleDhtContentHelper.batchInsert(selection);
        }
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        Log.i(TAG, "Input : " + input);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        Log.i(TAG, "sha1 : " + sha1);
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    public String getAllRedistMsgs(String clientPort) {
        Log.i(TAG, "Context : " + SimpleDhtProvider.context);
        SimpleDhtProvider.context.getContentResolver().update(mUri, null, clientPort, null);
        return SimpleDhtProvider.redisMsgs;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public void insertRedisMsgs(String receivedMsg) {
        SimpleDhtProvider.context.getContentResolver().update(mUri, null, receivedMsg, new String[1]);
    }

    public String insertSingleMsg(String key, String value) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(GeneralConstants.KEY, key);
        contentValues.put(GeneralConstants.VALUE, value);
        SimpleDhtProvider.context.getContentResolver().insert(mUri, contentValues);
        return "";
    }

    public String getResponseForQuery(String key) {
        Log.i(TAG, "Query for key : " + key);
        Cursor cursor = SimpleDhtProvider.context.getContentResolver().query(mUri, null, key, null, null);
        response = new StringBuilder();
        int count = 0;
        //if(cursor.moveToFirst()) {
        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
            count++;
            response.append(cursor.getString(0));
            response.append(",");
            response.append(cursor.getString(1));
            response.append(",");
            //cursor.moveToNext();
        }
        if (response.length() > 0 && response.charAt(response.length() - 1) == ',') {
            response = response.deleteCharAt(response.length() - 1);
        }
        Log.i(TAG, "Finished with cursor iteration. Returning : " + count + " rows" + " with value : " + response.toString());
        //  }
        return response.toString();
    }

    public void deleteKey(String key) {
        SimpleDhtProvider.context.getContentResolver().delete(mUri, key, null);
    }
}
