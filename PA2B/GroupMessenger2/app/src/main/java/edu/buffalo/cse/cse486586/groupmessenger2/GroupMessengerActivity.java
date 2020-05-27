package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.ArrayList;
import java.util.Comparator;


/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    String[] ports = {"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;

    ContentValues values = new ContentValues();
    int random_key_value = 0;
    int agreed_num = 0;
    int prop_num = 0;
    String failedNode="";

    //Priority Queue Comparator using sequence numbers and Port numbers
    Comparator<Message_format> queueComparator = new Comparator<Message_format>() {
        @Override
        public int compare(Message_format s1, Message_format s2) {
            if(s1.getsequencenum() > s2.getsequencenum()){
                return 1;
            }else if(s2.getsequencenum() > s1.getsequencenum()){
                return -1;
            }else{
                if(Integer.parseInt(s1.get_port()) > Integer.parseInt(s2.get_port())){
                    return 1;
                }else if(Integer.parseInt(s2.get_port()) > Integer.parseInt(s1.get_port())){
                    return -1;
                }
            }
            return 0;
        }
    };

    PriorityQueue<Message_format> queue = new PriorityQueue<Message_format>(50,queueComparator);

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        //Code referenced from PA1 to calculate the port number to listen on and to create a server socket
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        //Referenced From my PA2A
        final Button send = (Button) findViewById(R.id.button4);
        send.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                final EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                return;
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            synchronized (this) {
                try {
                    while (true) {

                        String input1;
                        Socket socket = serverSocket.accept();

                        PrintWriter output2 = new PrintWriter(socket.getOutputStream(), true);

                        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        if ((input1 = reader.readLine()) != null) {

                            Message_format msg = Message_format.toMessage(input1);

                             //Initial Message received when the flag is false the message is asking for the proposed numbers
                            if (msg.get_flag().equals("false") ) {

                                prop_num = (prop_num > agreed_num) ? (prop_num + 1) : (agreed_num + 1); //Calculating the proposed numbers using local prop num and agreed number

                                msg.setsequencenum(prop_num);

                                queue.add(msg); //adding the message to queue

                                output2.println(msg.toString()); //Replying back with proposed number

                            } else  {//When the flag is true, message is ready to be delivered

                                agreed_num = (agreed_num > msg.getsequencenum()) ? agreed_num : msg.getsequencenum(); // Updating the agreed number to keep track

                                prop_num = msg.getsequencenum();//Updating the local prop number

                                //Removing the old message which is already stored by using the message and its port number together as the unique id in if loop
                                Iterator<Message_format> itr = queue.iterator();
                                while (itr.hasNext()) {
                                    Message_format x = itr.next();
                                    if ((x.get_msg() + x.get_port()).equals(msg.get_msg() + msg.get_port()) && x.get_flag().equals("false")) {
                                        queue.remove(x);
                                    }
                                }
                                //Adding the new message to be delivered
                                queue.add(msg);
                                System.out.println(queue);

                                output2.println("Ack_again");//Used for setting the timeout exception in client side

                                //When the message is ready to be delivered, it is polled from the queue
                                while (queue.peek() != null && (queue.peek().get_flag().equals("true"))) {
                                    Log.e(TAG,"Message to be delivered is "+queue.peek().toString());
                                    publishProgress(queue.poll().get_msg());
                                }
                                //Removing the failed node messages from the queue
                                if (queue.peek() != null && (queue.peek().get_port().equals(failedNode))) {
                                    queue.poll();
                                }

                                socket.close();

                            }
                        }
                    }
                } catch (SocketTimeoutException e) {
                    Log.e(TAG, "ServerSide SocketTimeout Exception ");
                } catch (EOFException e) {
                    Log.e(TAG, "ServerSide EOF Exception");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return null;

            }
        }

        //Referenced from my PA2A
        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0];
            values.put("key",random_key_value);
            values.put("value", strReceived);
            //Referenced from PA2A project description
            getContentResolver().insert(
                    Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider"),
                    values
            );

            TextView text = (TextView) findViewById(R.id.textView1);
            text.append(strReceived + "\n");

            random_key_value++;
        }

    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

            Message_format msgToSend = null;
            int prop = 0;

            synchronized (this) {
                //Initial Multicast to get the proposal numbers from all the remaining servers.
                for (String port : ports) {
                    try {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));

                        socket.setSoTimeout(500);

                        msgToSend = new Message_format(msgs[0], prop, msgs[1], "false");//Initial message sent to servers

                        PrintWriter output1 = new PrintWriter(socket.getOutputStream(), true);

                        output1.println(msgToSend.toString());

                        BufferedReader input2 = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        msgToSend = Message_format.toMessage(input2.readLine());

                        prop = (msgToSend.getsequencenum() > prop) ? msgToSend.getsequencenum() : prop; // Calculating the agreed proposed number

                        if(input2.readLine()!=null) {
                            socket.close();
                        }
                        Thread.sleep(500);//Using this because sometimes it is not throwing the exception so that process waits

                    }catch(SocketTimeoutException e){
                        Log.e(TAG,"Client Side Socket Timeout Exception");
                    }
                    catch (Exception e) {
                        Log.e(TAG, "Exception Caused Due to " + e);
                        failedNode = port;//Catching the final port failed node here

                    }

                }

                //Deleting the failed node from the porta array
                Log.e(TAG,"Failed Node is "+failedNode);
                if (!failedNode.equals("")) {
                    ports = remove_node(failedNode);
                    System.out.println("New Ports Are" + Arrays.toString(ports));
                }
                msgToSend.setsequencenum(prop); //Setting the final agreed number for that message
                msgToSend.change_flag("true");//Changing the flag so now the message can be multicasted in final stage


                //Final Mulitcast of the Message
                for (String port : ports) {
                    try {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));

                        socket.setSoTimeout(500);

                        PrintWriter output3 = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader reader2 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        output3.println(msgToSend.toString());

                        if(reader2.readLine()!=null) {
                            socket.close();
                        }
                        Thread.sleep(500);//Using this because sometimes it is not throwing the exception so that process waits
                    }
                    catch(SocketTimeoutException e){
                        Log.e(TAG,"Client Side Socket Timeout Exception "+e);
                    }catch (Exception e) {
                        Log.e(TAG, "Exception Due to " + e);
                    }
                }

                return null;

            }
        }

    }

    //Function to remove a port from the ports array
    //Referenced from https://knpcode.com/java-programs/remove-element-from-an-array-in-java/
    public String[] remove_node(String failedNode) {
        List<String> tempList = new ArrayList(Arrays.asList(ports));
        tempList.remove(failedNode);
        return tempList.toArray(new String[0]);
    }


    //Message_format class
    public static class Message_format{
        private String msg;//content
        private int seq_num;//sequence number
        private String port_num;//port_number
        private String flag; // Indicated when can the message be delivered

        public Message_format(String msg, int seq_num, String port_num, String flag) {
            this.msg = msg;
            this.seq_num = seq_num;
            this.port_num = port_num;
            this.flag = flag;
        }

        //Getters and Setter for the variables of the Message class
        public String get_msg(){
            return msg;
        }

        public String get_port() {
            return port_num;
        }

        public int getsequencenum() {
            return seq_num;
        }

        public void setsequencenum(int seq) {
            this.seq_num = seq;
        }

        public String get_flag() {
            return flag;
        }

        public void change_flag(String flag) {
            this.flag = flag;
        }

        //Converting the object type into string
        @Override
        public String toString() {
            return msg+"~"+seq_num+"~"+port_num+"~"+flag;
        }

        //Converting the string to object type
        public static Message_format toMessage(String msg){
            String[] messages= msg.split("~");
            return new Message_format(messages[0], Integer.parseInt(messages[1]), messages[2], messages[3]);
        }

    }

}


