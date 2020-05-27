package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.Iterator;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.*;
import java.util.*;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    private final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private final String[] AVD_Ports = new String[]{"5554", "5556", "5558", "5560", "5562"};
    private final Integer SERVER_PORT = 10000;


    Node curNode = null;
    private String portStr = null;
    private List<Node> AVD_Nodes = new LinkedList<Node>();
    private Queue<String> failedNodes = new LinkedList<String>();
    private HashMap<String, Queue<String>>  Queries = new HashMap<String, Queue<String>>();
    private HashMap<String, Integer> Quorum = new HashMap<String, Integer>();

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        //setting the current node port
        curNode = new Node(portStr,null,null,null);

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch(IOException ioe){
            Log.e(TAG, "Exception encountered while creating server socket : " + ioe.getMessage());
            return false;
        }

        try {
            //Setting the Current Node Hash id
            curNode.setPort_id(genHash(curNode.getNode_port()));

            //Creating a AVD List and sorting them by the Hashed values
            for(int i =0;i<AVD_Ports.length;i++){
                String nodePort = AVD_Ports[i];
                Node node = new Node(nodePort,null,null,null);

                String port_id = genHash(node.getNode_port());
                node.setPort_id(port_id);

                AVD_Nodes.add(node);
            }

            Collections.sort(AVD_Nodes, new Comparator<Node>() {
                @Override
                public int compare(Node n1, Node n2) {
                    return n1.getPort_id().compareTo(n2.getPort_id());
                }
            });

            //Getting the Position of Currrent Node
            Iterator<Node> itr1 = AVD_Nodes.iterator();
            int i = 0;
            while(itr1.hasNext()) {
                Node h = itr1.next();
                if(h.getNode_port().equals(curNode.getNode_port())){
                    break;
                }
                i++;
            }
            curNode.setPos(i);

            //Setting the Predecessor ID for the Current Node
            int curPos = curNode.getPos();
            int lastPos = AVD_Nodes.size()-1;
            if(curPos == 0){
                curNode.setPred_id(AVD_Nodes.get(lastPos).getPort_id());
            }
            else{
                curNode.setPred_id(AVD_Nodes.get(curPos-1).getPort_id());
            }

        }
        catch (Exception e){
            Log.e(TAG, "Exception encountered in OnCreate While generating portid");
        }

        //https://developer.android.com/training/data-storage/app-specific
        //Storing each Node in File and Recovering the Node if not exists
        File AVD = new File(this.getContext().getFilesDir(), "AVD.txt");
        if(!AVD.exists()) {
            try {
                FileOutputStream outputStream = this.getContext().openFileOutput("AVD.txt", Context.MODE_PRIVATE);
                outputStream.write(("First Installation").getBytes());
                outputStream.close();
            }
            catch(Exception e){
                Log.e(TAG, "Caught during the First Installation");
            }
        }
        else{
            RecoverNode();
        }

        return true;
    }

    //Refered from my PA3 -- Wait for the query responses to come back
    public void wait_(String s){
        synchronized (s){
            try {
                s.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String)values.get("key");;
        try{
            Queue<Object> MainNode = GetMainNode(key);
            Node n1 = (Node) (MainNode.peek());
            MainNode.remove();
            int MainNode_Pos = (Integer) MainNode.peek();
            MainNode.remove();

            Node msg1 = new Node(curNode.getNode_port(),"I",values.get("key")+":"+values.get("value"),null);

            if(n1.getNode_port().equals(curNode.getPort_id())){
                Version(values);
                Quorum.put(key, 1);
                clientReplicas(curNode.getPos(), msg1.toString());//forward request to replicas
            }
            else{
                Quorum.put(key, 0);
                client(PortValue(n1.getNode_port()), msg1.toString());
                clientReplicas(MainNode_Pos, msg1.toString());//forward request to replicas
            }

            wait_(key);
        }catch (Exception e){
            Log.e(TAG, "Exception caught in Insert Operation for haskey"+key);
        }
        return uri;
    }

    //https://stackoverflow.com/questions/11440720/how-to-run-query-in-sqlite-database-in-android
    //Query command is refered from my PA2A and PA3
    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getReadableDatabase();

        Queue<String> resultsQ = new LinkedList<String>();
        //Local
        if(selection.equals("@")){
            resultsQ.add(Cur_to_Str(db.rawQuery("SELECT * FROM SQLtable", null)));
            Queries.put("@", resultsQ);
        }
        else {//Global
            if (selection.equals("*")) {
                Cursor cursor = db.rawQuery("SELECT * FROM SQLtable", null);
                resultsQ.add(Cur_to_Str(cursor));
                Queries.put(selection, resultsQ);

                Node msg1 = new Node(curNode.getNode_port(),"Q","*",null);

                int i=0;
                while(i<AVD_Nodes.size()){
                    if(i != curNode.getPos()){
                        String port = AVD_Nodes.get(i).getNode_port();
                        client(PortValue(port), msg1.toString());//forward request all other nodes
                    }
                    i++;
                }
            } else {//Specific results
                try {
                    Queue<Object> MainNode = GetMainNode(selection);
                    Node n1 = (Node) (MainNode.peek());
                    MainNode.remove();
                    int MainNode_Pos = (Integer) MainNode.peek();
                    MainNode.remove();

                    Node msg2 = new Node(curNode.getNode_port(),"Q",selection,null);

                    if (n1.getNode_port().equals(curNode.getNode_port())) {
                        Cursor cur = db.rawQuery("SELECT * FROM SQLtable WHERE key" + "=\"" + selection + "\"",null);
                        resultsQ.add(Cur_to_Str(cur));
                        clientReplicas(curNode.getPos(), msg2.toString());//forward request to replicas
                    } else {
                        client(PortValue(n1.getNode_port()), msg2.toString());
                        clientReplicas(MainNode_Pos, msg2.toString());//forward request to replicas
                    }
                    Queries.put(selection, resultsQ);
                } catch (Exception e){
                    Log.e(TAG, "Exception caught in Query Operation for haskey"+selection);
                }
            }
            wait_(selection);
        }
        db.close();
        dbHelper.close();

        Queue<String> results = QueriesResults(selection);
        return GetQueryresponses(results);
    }

    //https://stackoverflow.com/questions/11440720/how-to-run-query-in-sqlite-database-in-android
    //Delete Command Refered from my PA2A and PA3
    @Override
    public int delete(Uri uri, String sel, String[] selectionArgs) {
        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();

        //Local Partition Query Results
        if(sel.equals("@")){
            db.rawQuery("Delete from SQLtable", null);
        }
        //Global Partition Query Results
        else if(sel.equals("*")){
            db.rawQuery("Delete from SQLtable", null);

            Node msg1 = new Node(curNode.getNode_port(),"D","@",null);

            int i=0;
            while(i<AVD_Nodes.size()){
                if(i != curNode.getPos()){
                    String port = AVD_Nodes.get(i).getNode_port();
                    client(PortValue(port), msg1.toString());//forward request all other nodes
                }
                i++;
            }
        }

        else{
            try{//Deleting a specific key
                Queue<Object> MainNode = GetMainNode(sel);
                Node n1 = (Node)(MainNode.peek());
                MainNode.remove();
                int MainNode_Pos = (Integer) MainNode.peek();
                MainNode.remove();

                if(n1.getNode_port().equals(curNode.getNode_port())){
                    db.rawQuery("Delete from SQLtable WHERE key" + "=\"" + sel + "\"",null);
                    Quorum.put(sel, 1);

                    Node msg2 = new Node(curNode.getNode_port(),"D",sel,null);
                    clientReplicas(curNode.getPos(), msg2.toString());//forward request to replicas

                }
                else{
                    Node msg3 = new Node(curNode.getNode_port(),"D",sel,null);
                    Quorum.put(sel, 0);

                    client(PortValue(n1.getNode_port()), msg3.toString());
                    clientReplicas(MainNode_Pos, msg3.toString());//forward request to replicas
                }
                wait_(sel);
            }
            catch (Exception e){
                Log.e(TAG, "Exception caught in Delete Operation for haskey"+sel);
            }

        }
        db.close();
        dbHelper.close();
        return 0;
    }

    //Recovering the Failed Node data
    public void RecoverNode(){
            Node msg1 = new Node(curNode.getNode_port(),"QREP","FR","SR");
            msg1.setPos(curNode.getPos());
            Node msg2 = new Node(curNode.getNode_port(),"QREP","FR","APR");

            Queue<String> res1 = new LinkedList<String>();
            Queries.put("FR", res1);

            String succ = Successor(curNode.getPos()).getNode_port();

            client(PortValue(succ), msg1.toString());//Get the results from self

            int curNodepos = curNode.getPos(),i=0;
            while(i<2) {
                if (curNodepos == 0)
                    curNodepos = AVD_Nodes.size();
                curNodepos--;
                String predPort = AVD_Nodes.get(curNodepos).getNode_port();
                client(PortValue(predPort), msg2.toString());//Get the results from replicas
                i++;
            }
            wait_("FR");
            Queue<String> res2 = QueriesResults("FR");

            Iterator<Map.Entry<String, Node>> itr = VersionMapResults(res2).entrySet().iterator();
            while(itr.hasNext()) {
                Map.Entry<String, Node> entry = itr.next();
                String k = entry.getKey();
                String v = entry.getValue().getVal();
                String ver = String.valueOf(entry.getValue().getVer());
                ContentValues insertValues = new ContentValues();
                insertValues.put("key", k);
                insertValues.put("value", v);
                insertValues.put("version", Integer.parseInt(ver));
                Version(insertValues);
            }
    }

    //Insert Command is taken from the my PA2A and PA3
    //Inserting Version Column Value
    public void Version(ContentValues val) {
        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        SQLiteDatabase db1 = dbHelper.getReadableDatabase();
        Cursor cur = db1.rawQuery("SELECT * FROM SQLtable WHERE key" + "=\"" + val.get("key") + "\"",null);
        if(cur.getCount() != 0){
            cur.moveToFirst();
            String presentValue = cur.getString(cur.getColumnIndex("value"));
            Integer presentVer = cur.getInt(cur.getColumnIndex("version"));
            if(!val.get("value").equals(presentValue)) {
                if(val.containsKey("version")){
                    Integer newVer = (Integer) val.get("version");
                    if(presentVer <= newVer ) {//Check the already stored version value with ones sent by replicas if newver is greater, increase the version
                        val.put("version", ++presentVer);
                        db.insertWithOnConflict("SQLtable", null, val, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                }
                else {
                    val.put("version", ++presentVer);
                    db.insertWithOnConflict("SQLtable", null, val, SQLiteDatabase.CONFLICT_REPLACE);
                }
            }
        }
        else{
            val.put("version", 0);//new version value directly store it
            db.insertWithOnConflict("SQLtable", null, val, SQLiteDatabase.CONFLICT_REPLACE);
        }
        db1.close();;
        db.close();;
        dbHelper.close();
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
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

    //Get All the Results from Queries HashMap and refresh it
    public Queue<String> QueriesResults(String k){
        Queue<String> res = null;
        synchronized (Queries){
            res = Queries.get(k);
            Queries.remove(k);
        }
        return res;
    }

    //Refer from my PA3 --> Client Task
    public void client(String port, String m){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port, m);
    }

    //Getting the Node Successors
    public Node Successor(int pos){
        return AVD_Nodes.get((pos + 1) % AVD_Nodes.size());
    }

    //Sending the message to Replicas
    public void clientReplicas(int pos, String m) {
        Node succ1 = Successor(pos);
        Node succ2 = Successor(pos+1);
        client(PortValue(succ1.getNode_port()), m);
        client(PortValue(succ2.getNode_port()),m);
    }

    //Refered From my PA3 with extra version column - Converting Cursor to String
    public String Cur_to_Str(Cursor cursor){

        StringBuilder strbul=new StringBuilder();
        Queue<String> list=new LinkedList<String>();

        if (cursor.moveToFirst()) {
            int key = cursor.getColumnIndex("key");
            int value = cursor.getColumnIndex("value");
            int version = cursor.getColumnIndex("version");
            do {
                list.add(cursor.getString(key)+":"+cursor.getString(value)+":"+cursor.getString(version));
            } while (cursor.moveToNext());

            for(String str : list){
                strbul.append(str);
                strbul.append("/");
            }
            return strbul.toString();
        }
        return null;
    }

    //Get the Query Results from Versioned Map and add the row
    public Cursor GetQueryresponses(Queue<String> res){
        MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});
        Iterator<Map.Entry<String, Node>> itr = VersionMapResults(res).entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, Node> entry = itr.next();
            String k = entry.getKey();
            String v = entry.getValue().getVal();
            cur.newRow().add("key", k).add("value", v);
        }

        return cur;
    }

    //Filter the query results in range of the Node
    public Cursor FilterInRange(Cursor c, String Co_ID, String predID) {
        MatrixCursor cur = new MatrixCursor(new String[]{"key", "value", "version"});

        if(c.moveToFirst()){
            int k = c.getColumnIndex("key");
            int v = c.getColumnIndex("value");
            int ver = c.getColumnIndex("version");

            do{
                if(checkinLocal(Co_ID, predID, c.getString(k)) || checkinLast(Co_ID, predID, c.getString(k))) {
                    cur.newRow().add("key", c.getString(k)).add("value", c.getString(v)).add("version", c.getString(ver));
                }
            }while(c.moveToNext());
        }

        return cur;
    }

    //Versioning the results
    public Map<String, Node> VersionMapResults(Queue<String> res){
        Map<String, Node> ver_map = new HashMap<String, Node>();
        while(res.size()>0){
            if(res.peek()!=null && !res.peek().equals("null")){
                String result[] = res.peek().split("/");
                int i = 0;
                while (i < result.length) {
                    String k = result[i].split(":")[0];
                    String v = result[i].split(":")[1];
                    String ver = result[i].split(":")[2];
                    if(ver_map.containsKey(k)){
                        Node n1 = ver_map.get(k);
                        if(n1.getVer() < Integer.parseInt(ver)){
                            n1.setVal(v);
                            n1.setVer(Integer.valueOf(ver));
                        }
                    }
                    else{
                        Node n2 = new Node(null,null,null,null);//Dummy Node
                        n2.setVal(v);
                        n2.setVer(Integer.valueOf(ver));
                        ver_map.put(k, n2);
                    }
                    i++;
                }
            }
            res.remove();
        }

        return ver_map;
    }

    //Get the Main Coordinator Information
    public Queue<Object> GetMainNode(String k){
        Queue<Object> MainNode = new LinkedList<Object>();

        for(int c=0, p = AVD_Nodes.size()-1 ; c < AVD_Nodes.size(); c++, p++){
            Node pred = AVD_Nodes.get(p%AVD_Nodes.size());
            if(checkinLocal(AVD_Nodes.get(c).getPort_id(), pred.getPort_id(), k) || checkinLast(AVD_Nodes.get(c).getPort_id(), pred.getPort_id(), k)) {
                MainNode.add(AVD_Nodes.get(c));
                MainNode.add(c);
                return MainNode;
            }
        }
        return null;
    }

    //Refered from my PA3 --> Check in Local Partition
    public boolean checkinLocal(String c, String ID, String k){
        try {
            if (ID.compareTo(genHash(k)) < 0 && c.compareTo(genHash(k)) >= 0)
                return true;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    //Refered from my PA3-->Check in Last PartitioncurNode.getPort_id()
    public boolean checkinLast(String c, String ID, String k){

        try {
            if (ID.compareTo(c) > 0 && (ID.compareTo(genHash(k)) < 0 || c.compareTo(genHash(k)) >= 0))
                return true;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    //Portvalues of AVD's, i.e., 5554 --> 11108
    public String PortValue(String avdID) {
        return String.valueOf(Integer.parseInt(avdID) * 2);
    }

    //Inserting Message
    public void Insert_msg(Node msg){
        String key = msg.getKey().split(":")[0];
        String value = msg.getKey().split(":")[1];
        ContentValues insertValues = new ContentValues();
        insertValues.put("key",key);
        insertValues.put("value", value);
        Version(insertValues);

        Node msg1 = new Node(curNode.getNode_port(),"IA",key,null);
        client(PortValue(msg.getNode_port()), msg1.toString());
    }

    //Querying the Message
    public void Query_msg(Node msg){
        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cur = null;
        if(msg.getKey().equals("*") || msg.getKey().equals("@")) {
            cur = db.rawQuery("Select * from SQLtable", null);//local and global
        }
        else {
            cur = db.rawQuery("Select  * from SQLtable where `key` = ?",new String[]{msg.getKey()});//Specific Key
        }
        Node msg3 = new Node(curNode.getNode_port(),"QR",msg.getKey(),Cur_to_Str(cur));
        client(PortValue(msg.getNode_port()), msg3.toString());
        db.close();
        dbHelper.close();
    }

    //Query Response
    public void Query_response(Node msg){
        Queue<String> res = Queries.get(msg.getKey());
        if(res != null) {
            res.add(msg.getCur());

            if (msg.getKey().equals("*") && res.size() == (AVD_Nodes.size() - failedNodes.size())) {
                notify_("*");
            }
            if (msg.getKey().equals("FR") && res.size() == 3) {
                notify_("FR");
            }
            else if (!msg.getKey().equals("*") && !msg.getKey().equals("FR") && res.size() == 2) {
                notify_(msg.getKey());
            }
        }
    }

    //Delete Message
    public void Delete_msg(Node msg){
        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        db.delete("SQLtable","key = ?",new String[]{msg.getKey()});
        Node msg2 = new Node(curNode.getNode_port(),"DA",msg.getKey(),null);
        client(PortValue(msg.getNode_port()), msg2.toString());
        db.close();
        dbHelper.close();
    }

    //Query Replicas
    public void Query_Replicas(Node msg){
        SQLiteDbCreation dbHelper = new SQLiteDbCreation(getContext());
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cursor = db.rawQuery("Select * from SQLtable", null);

        //Getting Position of the node
        int curPos = msg.getPos();
        int lastPos = AVD_Nodes.size()-1;
        if(curPos == 0){
            msg.setPred_id(AVD_Nodes.get(lastPos).getPort_id());
        }
        else{
            msg.setPred_id(AVD_Nodes.get(curPos-1).getPort_id());
        }

        try {//Self Replica results
            if(msg.getCur().equals("SR")){
                cursor = FilterInRange(cursor, AVD_Nodes.get(msg.getPos()).getPort_id(), msg.getPred_id());
            }
            else if(msg.getCur().equals("APR")){
                cursor = FilterInRange(cursor, curNode.getPort_id(), curNode.getPred_id());//Other replicas results
            }
        }
        catch (Exception e){
            Log.e(TAG, "Exception encountered"+e+"while generating Hash in Query Response");
        }

        Node msg4 = new Node(curNode.getNode_port(),"QR",msg.getKey(),Cur_to_Str(cursor));
        client(PortValue(msg.getNode_port()), msg4.toString());
        db.close();
        dbHelper.close();
    }

    //Insert and Delete Acknowledgement from replicas
    public void Ack_Ins_Del(String key){
        Integer c = Quorum.get(key);
		Iterator<HashMap.Entry<String, Integer>> itr = Quorum.entrySet().iterator();
        if(c != null) {
            c++;
            if (c == 2) {//Refered from PA3
                while(itr.hasNext())
                {
                    HashMap.Entry<String, Integer> entry = itr.next();
                    String k = entry.getKey();
                    if (k.equals(key))
                        synchronized (k) {
                            k.notify();
                            Quorum.remove(k);
                        }
                }
            } else {
                Quorum.put(key, c);
            }
        }
    }

    //Refered from my PA3 -- notify to send the results back to the source
    public void notify_(String key){
        Iterator<HashMap.Entry<String, Queue<String>>> itr = Queries.entrySet().iterator();
        while(itr.hasNext())
        {
            HashMap.Entry<String, Queue<String>> entry = itr.next();
            String k = entry.getKey();
            if (k.equals(key))
                synchronized (k) {
                    k.notify();
                }
        }
    }

    //Refered from my PA3 --> Server Task
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            while(true){
                try{
                    Socket socket = serverSocket.accept();
                    DataInputStream server = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF("Acknowledgement");//Send back Ack to detect timeout

                    String in = server.readUTF();
                    Log.i(TAG,"Message received at Server side is "+in);
                    if(in != null){
                        Node msgRcv = Node.toMessage(in);

                        if(msgRcv.getOpt().equals("I")){
                            Insert_msg(msgRcv);
                        }
                        else if(msgRcv.getOpt().equals("D")){
                            Delete_msg(msgRcv);
                        }
                        else if(msgRcv.getOpt().equals("IA") || msgRcv.getOpt().equals("DA")){
                            Ack_Ins_Del(msgRcv.getKey());
                        }
                        else if(msgRcv.getOpt().equals("Q")){
                            Query_msg(msgRcv);
                        }
                        else if(msgRcv.getOpt().equals("QREP")){
                            Query_Replicas(msgRcv);
                        }
                        else if(msgRcv.getOpt().equals("QR")){
                            Query_response(msgRcv);
                        }
                    }
                }
                catch(Exception e){
                    Log.e(TAG,"Exception occured in Server Task");
                    e.printStackTrace();
                }
            }
        }
    }

    //Refered from My PA3 --> Client Task with Failure Detection
    private class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... msgs) {

            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(msgs[0]));
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());

                //Checking for timeout of the port by receiving the Acknowledgement
                while(true){
                    if(in.readUTF().equals("Acknowledgement"))
                        break;
                }
                Log.e(TAG,"Message sent from Client side is "+msgs[1]);
                out.writeUTF(msgs[1]);


                if(failedNodes.contains(msgs[0])) {
                    synchronized (failedNodes) {
                        failedNodes.remove(msgs[0]);//Removing the Failed ports
                    }
                }
            }
            catch(Exception e){
                Log.e(TAG, "Client Side Exception caused at port " + msgs[0]);
                if(!failedNodes.contains(msgs[0])) {
                    synchronized (failedNodes) {
                        failedNodes.add(msgs[0]);
                    }
                }//catching  the failed ports
            }

            return null;
        }
    }

    //Refered from my PA3
    //Node class
    public static class Node {
        private String node_port; // current_node port value
        private String Opt; //Options to Perform insert,delete,query,..
        private String Key;// Key
        private String val;//Value
        private int ver;//Version
        private String Cur; // Cursor
        private int pos = 0;//Position in the Ring
        private String port_id = null; // Current port hash value
        private String pred_id = null; // Predecessor port hash value

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

        public String getKey() {
            return Key;
        }

        public String getCur() {
            return Cur;
        }

        public int getPos(){
            return pos;
        }

        public void setPos(int pos){
            this.pos = pos;
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

       public String getVal(){
            return val;
       }

       public void setVal(String val){
            this.val = val;
       }

       public int getVer(){
            return ver;
       }

       public void setVer(int ver){
            this.ver = ver;
       }

        //Converting the object type into string
        @Override
        public String toString() {
            return node_port + "~" + Opt + "~" + Key + "~" + Cur+"~"+pos;
        }

        //Converting the string to object type
        public static Node toMessage(String msg) {
            String[] messages = msg.split("~");
            Node n = new Node(messages[ 0], messages[1], messages[2], messages[3]);
            n.setPos(Integer.parseInt(messages[4]));
            return n;
        }

    }
}
