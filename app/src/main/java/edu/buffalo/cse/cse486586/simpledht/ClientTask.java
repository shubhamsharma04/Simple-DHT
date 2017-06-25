package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;

/***
 * ClientTask is an AsyncTask that should send a string over the network.
 * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
 * an enter key press event.
 *
 * @author and Credit stevko
 */
public class ClientTask extends AsyncTask<String, Void, Void> {

    private static final String TAG = ClientTask.class.getName();

    @Override
    protected Void doInBackground(String... msgs) {
        int action = Integer.parseInt(msgs[0]);
        String myPort = msgs[1];
        Log.i(TAG, "Processing avd : " + myPort + " for action : " + action);
        String receivedMsg = "";
        String nextPort = "";
        String key = "";
        switch (action) {
            case GeneralConstants.ACTION_JOIN:
                receivedMsg = communicateWithServer(action, myPort, null, null, null);
               // receivedMsg = receivedMsg.split(",", 2)[1];
                String[] ports = new String[2];
                if(("").equals(receivedMsg)){
                    ports[0] = myPort;
                    ports[1] = myPort;

                } else {
                    // Update your own Predecessor & Successor
                    ports = receivedMsg.split(",", 2)[1].split(",");
                }
                Log.i(TAG, "Received response for action : " + action + ": " + receivedMsg);

                SimpleDhtProvider.prev = ports[0];
                SimpleDhtProvider.next = ports[1];
                Log.i(TAG,"MyPort : "+myPort+" pred : "+SimpleDhtProvider.prev+" succ : "+SimpleDhtProvider.next);
                // Update Others, provided there are others
                if(!myPort.equals(SimpleDhtProvider.prev)) {
                    action = GeneralConstants.ACTION_UPDATE_SUCC;
                    receivedMsg = communicateWithServer(action, myPort, ports[1], null, null);
                    Log.i(TAG, "Received response for action : " + action + ": " + receivedMsg);
                    action = GeneralConstants.ACTION_UPDATE_PRED;
                    receivedMsg = communicateWithServer(action, myPort, ports[0], null, null);
                    Log.i(TAG, "Received response for action : " + action + ": " + receivedMsg);
                    // Redistribute Messages
                    action = GeneralConstants.ACTION_REDISTRIBUTE_MSGS;
                    receivedMsg = communicateWithServer(action, myPort, ports[1], null, null);
                    receivedMsg = receivedMsg.split(",", 2)[1];
                    insertRedisMsgs(receivedMsg);
                    Log.i(TAG, "Received response for action : " + action + ": " + receivedMsg);
                }
                break;

            case GeneralConstants.ACTION_INSERT:
                nextPort = msgs[2];
                key = msgs[3];
                String value = msgs[4];
                Log.i(TAG,"Sending key : "+key+" value : "+value+" to port : "+nextPort);
                receivedMsg = communicateWithServer(action, myPort, nextPort, key, value);
                Log.i(TAG, "Received response for action : " + action + " : " + receivedMsg);
                break;
            case GeneralConstants.ACTION_QUERY:
                nextPort = msgs[2];
                key = msgs[3];
                Log.i(TAG,"Querying key : "+key+" with port : "+nextPort);
                // Store this somewhere
                receivedMsg = communicateWithServer(action,myPort,nextPort,key,"");
                SimpleDhtProvider.response = new StringBuilder(receivedMsg.replaceAll(GeneralConstants.ACK_MSG+",",""));
                Log.i(TAG, "Received response for action : " + action + " : " + SimpleDhtProvider.response.toString());
                break;
            case GeneralConstants.ACTION_NUCLEAR_QUERY:
                nextPort = msgs[2];
                key = msgs[3];
                Log.i(TAG,"Nuclear key : "+key+" with port : "+nextPort);
                receivedMsg = communicateWithServer(action,myPort,nextPort,key,"");
                SimpleDhtProvider.response = new StringBuilder(receivedMsg.replaceAll(GeneralConstants.ACK_MSG+",",""));
                Log.i(TAG, "Received Nuclear response for action : " + action + " : " + SimpleDhtProvider.response.toString());
                break;
            case GeneralConstants.ACTION_DELETE:
                nextPort = msgs[2];
                key = msgs[3];
                Log.i(TAG,"Sending key : "+key+" to be deleted");
                receivedMsg = communicateWithServer(action, myPort, nextPort, key, "");
                Log.i(TAG, "Received response for action : " + action + " : " + receivedMsg);
                break;
            case GeneralConstants.ACTION_NUCLEAR_DELETE:
                nextPort = msgs[2];
                key = msgs[3];
                Log.i(TAG,"Nuclear delete key : "+key+" with port : "+nextPort);
                receivedMsg = communicateWithServer(action,myPort,nextPort,key,"");
                Log.i(TAG, "Received response for action : " + action + " : " + receivedMsg);
                break;
        }


        return null;
    }


    private String communicateWithServer(int action, String myPort, String rmtPort, String key, String value) {
        Log.i(TAG, "Inside communicateWithServer with action : " + action);
        String result = "";
        String remotePort = "";
        JSONObject jsonObject = new JSONObject();
        try {
            switch (action) {
                case GeneralConstants.ACTION_JOIN:
                    remotePort = String.valueOf(2 * GeneralConstants.BASE_EMULATOR_ID);
                    break;
                default:
                    remotePort = rmtPort;
                    jsonObject.put(GeneralConstants.KEY, key);
                    jsonObject.put(GeneralConstants.VALUE, value);
                    break;
            }
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));
// Credit : Socket programming Based on the input from TA Sharath during recitation
            // socket.setSoTimeout(GeneralConstants.SOCKET_TIMEOUT);
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            jsonObject.put(GeneralConstants.ACTION, String.valueOf(action));
            jsonObject.put(GeneralConstants.CLIENT_ID, myPort);
            String msg = jsonObject.toString();
            Log.i(TAG, "Sending message : " + msg + " to server @ : " + remotePort);
            outputStream.writeUTF(msg);
            StringBuilder str = null;
            InputStream inputStream = socket.getInputStream();
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            do {
                str = new StringBuilder(dataInputStream.readUTF());
            } while (!str.toString().startsWith(GeneralConstants.ACK_MSG));
            Log.i(TAG, "Received msg : " + str.toString());
            result = str.toString();
            outputStream.close();
            inputStream.close();
            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "", e);
        } catch (JSONException e) {
            Log.e(TAG, "", e);
        }
        return result;
    }

    private void insertRedisMsgs(String receivedMsg) {
        new SimpleDhtProvider().insertRedisMsgs(receivedMsg);
    }

}