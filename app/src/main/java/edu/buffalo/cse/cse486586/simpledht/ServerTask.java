package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/***
 * ServerTask is an AsyncTask that should handle incoming messages. It is created by
 * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
 * <p>
 * Please make sure you understand how AsyncTask works by reading
 * http://developer.android.com/reference/android/os/AsyncTask.html
 *
 * @author and Credit stevko
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    private static final Integer sem = 0;

    private static final String TAG = ServerTask.class.getName();

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        Log.i(TAG, "Inside receive");
        while (true) {
            try {
                Socket client = serverSocket.accept();
                // client.setSoTimeout(GeneralConstants.SOCKET_TIMEOUT);
                // Credit : Socket programming Based on the input from TA Sharath during recitatio
                DataInputStream inputStream = new DataInputStream(client.getInputStream());
                StringBuilder str = new StringBuilder(inputStream.readUTF());
                Log.i(TAG,"Msg Received : "+str.toString());
                StringBuilder messageToSend = new StringBuilder(GeneralConstants.ACK_MSG);

                JSONObject jsonObject = new JSONObject(str.toString());
                int action = Integer.parseInt(jsonObject.getString(GeneralConstants.ACTION));
                String clientPort = jsonObject.getString(GeneralConstants.CLIENT_ID);
                String key = "";
                switch (action){
                    case GeneralConstants.ACTION_JOIN:
                        // A node wants to join. Return it's successor and predecessor
                       // int clientIndex = ((Integer.valueOf(clientPort)/2) - GeneralConstants.BASE_EMULATOR_ID)/2;
                        synchronized (sem) {
                            int clientIndex = getClientIndex(clientPort);
                            Log.v(TAG, "Client Index is : " + clientIndex);
                            SimpleDhtProvider.isAlive[clientIndex] = true;
                            // Set Base emul as alive
                            // TODO : Change this
                            SimpleDhtProvider.isAlive[2] = true;
                            messageToSend.append(",");
                            String port = "";
                            int length = SimpleDhtProvider.hashedNodes.size();
                            for (int i = clientIndex - 1; i != clientIndex; i--) {
                                if (i < 0) {
                                    i = length - 1;
                                }
                                if (SimpleDhtProvider.isAlive[i]) {
                                    Log.i(TAG, "Found pred index : " + i + " for Client Index : " + clientIndex);
                                    port = (SimpleDhtProvider.nodeToHashMap.get(SimpleDhtProvider.hashedNodes.get(i)));
                                    break;
                                }
                            }
                       /* if(("").equals(port)){
                            // only 5554 up
                            port = String.valueOf(GeneralConstants.BASE_EMULATOR_ID*2);
                        }*/
                            messageToSend.append(port);
                            messageToSend.append(",");
                            for (int i = clientIndex + 1; i != clientIndex; i++) {
                                if (i == length) {
                                    i = 0;
                                }
                                if (SimpleDhtProvider.isAlive[i]) {
                                    Log.i(TAG, "Found succ index : " + i + " for client index : " + clientIndex);
                                    port = (SimpleDhtProvider.nodeToHashMap.get(SimpleDhtProvider.hashedNodes.get(i)));
                                    break;
                                }
                            }
                       /* if(("").equals(port)){
                            // only 5554 up
                            port = String.valueOf(GeneralConstants.BASE_EMULATOR_ID*2);
                        }*/
                            messageToSend.append(port);
                        }
                        break;
                    case GeneralConstants.ACTION_UPDATE_SUCC:
                        SimpleDhtProvider.prev = clientPort;
                        Log.i(TAG,"Mine_succ : "+SimpleDhtProvider.myPort+"Prev : "+SimpleDhtProvider.prev+" Next : "+SimpleDhtProvider.next);
                        break;
                    case GeneralConstants.ACTION_UPDATE_PRED:
                        SimpleDhtProvider.next = clientPort;
                        Log.i(TAG,"Mine : "+SimpleDhtProvider.myPort+"Prev : "+SimpleDhtProvider.prev+" Next : "+SimpleDhtProvider.next);
                        break;
                    case GeneralConstants.ACTION_REDISTRIBUTE_MSGS:
                        String allRedisMsgs = getAllRedistMsgs(clientPort);
                        messageToSend.append(",").append(allRedisMsgs);
                        break;
                    case GeneralConstants.ACTION_INSERT:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        String value = jsonObject.getString(GeneralConstants.VALUE);
                        String insertMsgs = insertMsgs(key,value);
                        Log.i(TAG,"Insert single msg for Key : "+key+" Value : "+value);
                        break;
                    case GeneralConstants.ACTION_QUERY:
                       // messageToSend = new StringBuilder();
                        key = jsonObject.getString(GeneralConstants.KEY);
                        String response = getResponseForQuery(key);
                        //messageToSend.append(response);
                        messageToSend.append(",").append(response);
                        Log.i(TAG,"Query for Key : "+key+" got response : "+response);
                        break;
                    case GeneralConstants.ACTION_NUCLEAR_QUERY:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        String nucResponse = getResponseForQuery(key);
                        messageToSend.append(",").append(nucResponse);
                        Log.i(TAG,"Query for Nuclear Key : "+key+" got nucResponse : "+nucResponse);
                        break;
                    case GeneralConstants.ACTION_DELETE:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        deleteMsgs(key);
                        Log.i(TAG,"Delete for Key : "+key+" processed @ :  "+SimpleDhtProvider.myPort);
                        break;
                    case GeneralConstants.ACTION_NUCLEAR_DELETE:
                        key = jsonObject.getString(GeneralConstants.KEY);
                        deleteMsgs(key);
                        Log.i(TAG,"Delete for Key : "+key+" processed @ :  "+SimpleDhtProvider.myPort);
                        break;
                }
                DataOutputStream dataOutputStream = new DataOutputStream(client.getOutputStream());
                dataOutputStream.writeUTF(messageToSend.toString());
                dataOutputStream.close();
                inputStream.close();
                client.close();
            } catch (IOException e) {
                Log.e(TAG, "Can't connect ",e);
            } catch (JSONException e) {
                Log.e(TAG,"",e);
            }
        }
    }

    private void deleteMsgs(String key) {
        new SimpleDhtProvider().deleteKey(key);
    }

    private int getClientIndex(String clientPort) {
        Log.i(TAG,"Received clientPort : "+clientPort);
        String clientHashedPort = "";
        try {
            clientHashedPort = genHash(String.valueOf(Integer.parseInt(clientPort)/2));
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "", e);
        }
        Log.i(TAG,"Calculated clientHashedPort : "+clientHashedPort);
        int length = SimpleDhtProvider.hashedNodes.size();
        int index = -1;
        for (int i = 0; i < length; i++) {
            if (clientHashedPort.equals(SimpleDhtProvider.hashedNodes.get(i))) {
                index = i;
                break;
            }
        }
        return index;
    }

    private String getResponseForQuery(String key) {
        String result = "";
        result = new SimpleDhtProvider().getResponseForQuery(key);
        return result;
    }

    private String insertMsgs(String key, String value) {
        String result = "";
        result = new SimpleDhtProvider().insertSingleMsg(key,value);
        return result;
    }

    private String getAllRedistMsgs(String clientPort) {
        String result = "";
        result = new SimpleDhtProvider().getAllRedistMsgs(clientPort);
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

}
