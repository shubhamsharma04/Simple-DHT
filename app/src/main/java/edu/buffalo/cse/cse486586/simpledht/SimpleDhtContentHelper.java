package edu.buffalo.cse.cse486586.simpledht;

import android.app.Application;
import android.content.Context;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by opensam on 3/19/17.
 */

public class SimpleDhtContentHelper extends Application {
    private static final String TAG = SimpleDhtContentHelper.class.getName();

    // Credit : How to get context in non-activity class in a good way http://stackoverflow.com/questions/22371124/getting-activity-context-into-a-non-activity-class-android
    private static Application instance;


    // This would be populated by MainActivity
    public static String emulatorId = null;
    // This would be populated by MainActivity
    public static String myPort = null;
    // This would be populated by this class
    public static String myID = null;

    private static Map<String, String> map;

    private static Semaphore binaryLock;

    @Override
    public void onCreate() {
        Log.i(TAG, "SimpleDhtContentHelper created");
        super.onCreate();
        instance = this;
        map = new HashMap<String, String>();
        binaryLock = new Semaphore(1);
        Log.i(TAG, "instance created : " + instance);

    }


    public boolean writeContent(String key, String value) {
        boolean result = true;
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                Log.i(TAG, "Lock acquired for writing");
                map.put(key, value);
                break;
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result + " for key : " + key + " with value : " + value);
        return result;
    }

    public Map<String, String> getContentForKey(String key, int action) {
        Map<String, String> result = new HashMap<String, String>();
        //Log.i(TAG,"key : "+key+" action : "+action);
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                //Log.i(TAG,"Lock acquired for reading");
                if (GeneralConstants.LOCAL_ID == action) {
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    String value = map.get(key);
                    Log.i(TAG, "Returning value : " + value + " for key : " + key);
                    result.put(key, value);
                }
                break;
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result + " for key : " + key + " with action : " + action);
        return result;
    }


    public String deleteContentForKey(String key, int action) {
        String result = "S";
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                //Log.i(TAG,"Lock acquired for reading");
                if (GeneralConstants.LOCAL_ID == action) {
                    map = new HashMap<String, String>();
                } else {
                    result = map.remove(key);
                }
                break;
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
            result = "F";
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + result + " for key : " + key + " with action : " + action);
        return result;
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

    public String updateContentForKey(String key) {
        StringBuilder str = new StringBuilder();
        List<String> keysToBeDeleted = new ArrayList<String>();
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String mappedKey = "";
                    mappedKey = genHash(entry.getKey());
                    if (key.compareTo(mappedKey) > 0) {
                        str.append(entry.getKey()).append(",").append(entry.getValue());
                        keysToBeDeleted.add(entry.getKey());
                    }
                }
                for (String keyToBeDeleted : keysToBeDeleted) {
                    map.remove(keyToBeDeleted);
                }
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "", e);
        } finally {
            binaryLock.release();
        }
        Log.i(TAG, "Returning : " + str.toString() + " for key : " + key);
        return str.toString();
    }

    public void batchInsert(String selection) {
        try {
            while (binaryLock.tryAcquire(GeneralConstants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
               String [] keyValPairs = selection.split(",");
                int length = keyValPairs.length;
                Log.i(TAG,"Length of KeyValPairs Arr : "+length);
                if(length>1){
                    for(int i=0;i<length;i=i+2){
                        map.put(keyValPairs[i],keyValPairs[i+1]);
                    }
                }
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "", e);
        } finally {
            binaryLock.release();
        }
    }
}
