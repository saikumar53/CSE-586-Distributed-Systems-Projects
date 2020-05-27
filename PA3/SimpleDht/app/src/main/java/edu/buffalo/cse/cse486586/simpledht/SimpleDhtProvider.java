package edu.buffalo.cse.cse486586.simpledht;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java .util.*;

import android.database.sqlite.SQLiteDatabase;

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
    private final String TAG = SimpleDhtProvider.class.getSimpleName();
    private final Integer SERVER_PORT = 10000;

    private final String AVD_0 = "5554";
    ContentValues insertValues = new ContentValues();


    Node curNode=null;
    private String portStr = null;
    private String myportid = null;

    private HashMap<String,Queue<String>> Queries = new HashMap<String, Queue<String>>();

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException ioe) {
            Log.e(TAG, "Error encountered while creating server socket");
            return false;
        }

        try {
            myportid = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        curNode = new Node(portStr,"null",portStr,null);
        curNode.setPort_id(myportid);

        if (!portStr.equals(AVD_0)) {
            curNode.setOpt("J"); //Joining  Operation sent to 5554
            client(PortValue(AVD_0), curNode.toString());
        }

        return true;
    }

    //Insert Command is taken from the my PA2A
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v(TAG, "AVD " + curNode.getNode_port() + " : Insert - " + values.toString());

        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String key = (String) values.get("key");

        //If belongs to partition then insert
        if (checkFirst() || checkinLocal(key) || checkinLast(key)) {
            db.insertWithOnConflict("SQLtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            dbHelper.close();
            db.close();
        } else {
            //Forward to successor
            Node insert_msg = new Node(curNode.getNode_port(),"I",values.get("key") + ":" + values.get("value"),null);
            client(curNode.getSucc_port(), insert_msg.toString());
        }
        return uri;
    }

    //Wait for the query response to come back
    public void wait(String s){
        synchronized (s){
            try {
                s.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }



    //https://stackoverflow.com/questions/11440720/how-to-run-query-in-sqlite-database-in-android
    //Query command is refered from my PA2A
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getReadableDatabase();

        //local partition Query
        if (selection.equals("@")) {

            return db.rawQuery("SELECT * FROM SQLtable", null);
        }
        //Global Partition Query
        else if (selection.equals("*")) {

                Cursor cursor = db.rawQuery("SELECT * FROM SQLtable",null);

                Queue<String> q = new LinkedList<String>();
                q.add(Cur_to_Str(cursor));
                Queries.put("*", q);

                if (curNode.getSucc_port() != null) {
                    Node query_msg = new Node(curNode.getNode_port(), "Q", "*", null);
                    client(curNode.getSucc_port(), query_msg.toString());
                    wait("*");
                }

                return Str_to_Cur(Queries.get("*"));
        } else {
            if (checkFirst() || checkinLocal(selection) || checkinLast(selection)) {

                return db.rawQuery("SELECT * FROM SQLtable WHERE key" + "=\"" + selection + "\"",null);

            } else {//Forward to Successor if query not belongs to current partition
                if (curNode.getSucc_port() != null) {
                    Queue<String> queryResults = new LinkedList<String>();
                    Queries.put(selection, queryResults);

                    Node query_msg2 = new Node(curNode.getNode_port(), "Q", selection, null);
                    client(curNode.getSucc_port(), query_msg2.toString());
                    wait(selection);

                    return Str_to_Cur(Queries.get(selection));
                }

            }
        }
        dbHelper.close();
        db.close();
        return null;
    }

   //https://stackoverflow.com/questions/11440720/how-to-run-query-in-sqlite-database-in-android
    //Refered from my PA2A
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(TAG, "AVD " + curNode.getNode_port() + " Delete - " + selection);

        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();

        //Local Delete Query
        if (selection.equals("@")) {
            Cursor cur = db.rawQuery("Delete from SQLtable", null);

            return cur.getCount();
        }
        //Global Delete Query
        else if (selection.equals("*")) {
            Cursor cur = db.rawQuery("Delete from SQLtable",null);

            Node delete_msg = new Node(curNode.getNode_port(),"D","*",null);
            client(curNode.getSucc_port(), delete_msg.toString());
            return cur.getCount();
        }
        else {
            if (checkFirst() || checkinLocal(selection) || checkinLast(selection)) {
                Cursor cur = db.rawQuery("Delete from SQLtable WHERE key" + "=\"" + selection + "\"",null);
                return cur.getCount();
            } else {
                //Forward to Successor if query not belongs to current partition
                Node delete_msg2 = new Node(curNode.getNode_port(),"D",selection,null);
                client(curNode.getSucc_port(), delete_msg2.toString());
            }
        }
        dbHelper.close();
        db.close();
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    //Client Task
    public void client(String port, String m) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port, m);
    }

    //Portvalues of AVD's, i.e., 5554 --> 11108
    public String PortValue(String avdID) {
        return String.valueOf(Integer.parseInt(avdID) * 2);
    }

    //Used to check in first partition
    public boolean checkFirst(){
        if (curNode.getPred_id() == null && curNode.getSucc_port() == null) {
            return true;
        }
        return false;
    }

    //Check in Local Partition
    public boolean checkinLocal(String k){
        try {
            if (curNode.getPred_id().compareTo(genHash(k)) < 0 && (genHash(k)).compareTo(curNode.getPort_id()) <= 0)
                return true;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    //Check in Last PartitioncurNode.getPort_id()
    public boolean checkinLast(String k){

        try {
            if (curNode.getPred_id().compareTo(curNode.getPort_id()) > 0 && (curNode.getPred_id().compareTo(genHash(k)) < 0 || (curNode.getPort_id()).compareTo(genHash(k)) >= 0))
                return true;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    //Converting a String to Cursor
    public Cursor Str_to_Cur(Queue<String> res) {
        MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});

        while(res.size()>0){
            if(res.peek()!=null && !res.peek().equals("null")){
                String result[] = res.peek().split("/");
                int i = 0;
                while (i < result.length) {
                    String key = result[i].split(":")[0];
                    String value = result[i].split(":")[1];
                    cur.newRow().add("key", key).add("value", value);
                    i++;
                }
            }
            res.remove();
        }
        return cur;
    }


    //Converting Cursor to String
    public String Cur_to_Str(Cursor cursor) {

        StringBuilder strbul=new StringBuilder();
        List<String> list=new ArrayList<String>();

        if (cursor.moveToFirst()) {
            int key = cursor.getColumnIndex("key");
            int value = cursor.getColumnIndex("value");
            do {
                list.add(cursor.getString(key)+":"+cursor.getString(value));
            } while (cursor.moveToNext());

            for(String str : list){
                strbul.append(str);
                strbul.append("/");
            }
            return strbul.toString();
        }

        return null;
    }

    //Joining the Node on 5554
    public void JoinNode(Node msg) throws NoSuchAlgorithmException {
        if (curNode.getPred_id() == null && curNode.getSucc_port() == null) {
            //First Node join
            curNode.setSucc_port(PortValue(msg.getKey()));

            Node msg1 = new Node(curNode.getNode_port(), "S", curNode.getNode_port(), null);
            Node msg2 = new Node(curNode.getNode_port(), "P", curNode.getNode_port(), null);

            client(curNode.getSucc_port(), msg1.toString());
            client(curNode.getSucc_port(), msg2.toString());
        } else {
            if (checkFirst() || checkinLocal(msg.getKey()) || checkinLast(msg.getKey())) {


                Node msg3 = new Node(curNode.getNode_port(), "S", msg.getKey(), null);
                Node msg4 = new Node(curNode.getNode_port(), "S", curNode.getNode_port(), null);

                client(curNode.getPred_port(), msg3.toString());// Making the newly joined node as successor to the current node's predecessor
                client(PortValue(msg.getKey()), msg4.toString());//Making newly joined node successor as current node

                Update_PrevNode(msg);
            } else {
                //if node doesn't belong any of the partition forward it to next node to join
                client(curNode.getSucc_port(), msg.toString());
            }
        }
    }

    //Updating the Next Node
    public void Update_NextNode(Node msg){
        curNode.setSucc_port(PortValue(msg.getKey()));

        Node msg4 = new Node(curNode.getNode_port(), "P", curNode.getNode_port(), null);
        client(curNode.getSucc_port(), msg4.toString());
    }

    //Updating the Previous Node
    public void Update_PrevNode(Node msg) throws NoSuchAlgorithmException {
        curNode.setPred_id(genHash(msg.getKey()));
        curNode.setPred_port(PortValue(msg.getKey()));

    }

    //Insert Message
    public void Insert_msg(Node msg){
        String key = msg.getKey().split(":")[0];
        String value = msg.getKey().split((":"))[1];

        //if message belongs to current partition
        if (checkFirst() || checkinLocal(key) || checkinLast(key)) {
            insertValues.put("key", key);
            insertValues.put("value", value);
            insert(null, insertValues);
        } else {
            //forward the message to next partition
            client(curNode.getSucc_port(), msg.toString());
        }
    }

    //Query Message
    public void Query_msg(Node msg){
        if (msg.getKey().equals("*")) {
            Cursor cursor = query(null, null, "@", null, null);

            Node msg5 = new Node(curNode.getNode_port(), "QR", msg.getKey(), Cur_to_Str(cursor));

            client(PortValue(msg.getNode_port()), msg5.toString());

            client(curNode.getSucc_port(), msg.toString());

        } else {
            if (checkFirst() || checkinLocal(msg.getKey()) || checkinLast(msg.getKey())) {
                Cursor cursor = query(null, null, msg.getKey(), null, null);

                Node msg6 = new Node(curNode.getNode_port(), "QR", msg.getKey(), Cur_to_Str(cursor));
                client(PortValue(msg.getNode_port()), msg6.toString());

            } else {
                //Forward to next port
                client(curNode.getSucc_port(), msg.toString());
            }

        }
    }

    //Query Response
    public void Query_response(Node msg){
        Queue<String> res = Queries.get(msg.getKey());
        res.add(msg.getCur());

        if (!msg.getKey().equals("*")){
            notify_(msg);
        }

    }

    //Delete Message
    public void Delete_msg(Node msg){
        if (msg.getKey().equals("*")) {

            delete(null, "@", null);

            client(curNode.getSucc_port(), msg.toString());

        } else if(msg.getKey().equals("@")){

            if (checkFirst() || checkinLocal(msg.getKey()) || checkinLast(msg.getKey())) {

                delete(null, msg.getKey(), null);
            } else {
                //Forward to next partition
                client(curNode.getSucc_port(), msg.toString());
            }
        }
    }

    //Sneding the results back to the source
    public void notify_(Node msg){
        Iterator<HashMap.Entry<String, Queue<String>>> itr = Queries.entrySet().iterator();

        while(itr.hasNext())
        {
            HashMap.Entry<String, Queue<String>> entry = itr.next();
            String key = entry.getKey();
            if (key.equals(msg.getKey()))
                synchronized (key) {
                    key.notify();
                }
        }
    }



    //Server Task PA1 and PA2A
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream server = new DataInputStream(socket.getInputStream());
                    String in = server.readUTF();
                    if (in != null) {
                        Node msgRcv = Node.toMessage(in);

                        if (!msgRcv.getNode_port().equals(curNode.getNode_port())) {
                            if (msgRcv.getOpt().equals("J")) {
                                JoinNode(msgRcv);
                            } else if (msgRcv.getOpt().equals("S")) {
                                Update_NextNode(msgRcv);
                            } else if (msgRcv.getOpt().equals("P")) {
                                Update_PrevNode(msgRcv);
                            } else if (msgRcv.getOpt().equals("I")) {
                                Insert_msg(msgRcv);
                            } else if (msgRcv.getOpt().equals("Q")) {
                                Query_msg(msgRcv);
                            } else if (msgRcv.getOpt().equals("QR")) {
                                Query_response(msgRcv);
                            } else if (msgRcv.getOpt().equals("D")) {
                                Delete_msg(msgRcv);
                            }
                        }
                         else {
                            notify_(msgRcv);
                        }
                    }
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Exception occured in ServerTask");
                    e.printStackTrace();
                }
            }

        }


    }

    //Client Task refered from my PA1 and PA2A
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(msgs[0]));
                DataOutputStream client = new DataOutputStream(socket.getOutputStream());
                client.writeUTF(msgs[1]);
                socket.close();
            } catch (Exception e) {
                Log.d(TAG, "Exception encountered in ClientTask");
                e.printStackTrace();
            }
            return null;
        }
    }

    //Node class
    public static class Node {
        private String node_port; // current_node port value
        private String Opt; //Options to Perform join,insert,delete,query,..
        private String Key; // Key to be stored
        private String Cur; // Value to be stored

        private String port_id = null; // Current port hash value
        private String pred_id = null; // Predecessor port hash value
        private String succ_port = null; //Successort port value
        private String pred_port = null; // Predecessor port value

        public Node(String node_port, String Opt, String key, String c) {
            this.node_port = node_port;
            this.Opt = Opt;
            this.Key = key;
            this.Cur = c;
        }

        //Getters and Setter for the variables of the Node class
        public String getNode_port() {
            return node_port;
        }

        public String getOpt() {
            return Opt;
        }
        public void setOpt(String s) {
            this.Opt = s;
        }

        public String getKey() {
            return Key;
        }

        public String getCur() {
            return Cur;
        }

        public String getPort_id(){
            return port_id;
        }
        public void setPort_id(String s){
            this.port_id = s;
        }

        public String getPred_id(){
            return pred_id;
        }
        public void setPred_id(String s){
            this.pred_id = s;
        }
        public String getSucc_port(){
            return succ_port;
        }
        public void setSucc_port(String s){
            this.succ_port = s;
        }
        public String getPred_port(){
            return pred_port;
        }
        public void setPred_port(String s){
            this.pred_port = s;
        }

        //Converting the object type into string
        @Override
        public String toString() {
            return node_port + "~" + Opt + "~" + Key + "~" + Cur;
        }

        //Converting the string to object type
        public static Node toMessage(String msg) {
            String[] messages = msg.split("~");
            return new Node(messages[ 0], messages[1], messages[2], messages[3]);
        }

    }
}