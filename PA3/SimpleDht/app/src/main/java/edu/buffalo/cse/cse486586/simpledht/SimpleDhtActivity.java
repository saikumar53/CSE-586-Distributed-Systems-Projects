package edu.buffalo.cse.cse486586.simpledht;

import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;
import android.widget.Button;
import android.view.View;

public class SimpleDhtActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        //https://developer.android.com/reference/android/widget/Button
        final Button Gdump = (Button) findViewById(R.id.button2);
        Gdump.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                getContentResolver().query(
                Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"),
                null,
                "*",
                null,
                null);

            }
        });


        //https://developer.android.com/reference/android/widget/Button
        final Button Ldump = (Button) findViewById(R.id.button1);
        Ldump.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                getContentResolver().query(
                Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"),
                null,
                "@",
                null,
                null);
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
