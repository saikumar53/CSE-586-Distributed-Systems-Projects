package edu.buffalo.cse.cse486586.simpledht;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.content.Context;

//Refered from PA2A

//https://developer.android.com/training/data-storage/sqlite
//Creating a table called 'SQLtable'

public class SQLiteDbCreation extends SQLiteOpenHelper {
    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "FeedReader";
    public static final String table_name = "SQLtable";


    public SQLiteDbCreation(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("Create table "+table_name+" (`key` varchar primary key,value varchar)");
    }
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        ;
    }
}