package info.freelibrary.sodbox.impl;

import java.io.*;
import java.net.*;

import info.freelibrary.sodbox.*;


public class ReplicationDynamicSlaveStorageImpl extends ReplicationSlaveStorageImpl
{
    public ReplicationDynamicSlaveStorageImpl(String host, int port, String pageTimestampFilePath) { 
        super(pageTimestampFilePath);
        this.host = host;
        this.port = port;
    }

    public void open(IFile file, long pagePoolSize) {
        initialized = false;
        prevIndex = -1;
        outOfSync = true;
        super.open(file, pagePoolSize);
    }

    Socket getSocket() throws IOException {
        return new Socket(host, port);
    }

    protected String host;
    protected int    port;
}    

    
                                               