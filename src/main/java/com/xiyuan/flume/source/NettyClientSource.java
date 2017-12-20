package com.xiyuan.flume.source;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.xiyuan.flume.serialization.HeaderAndBodySerialization;
import org.apache.avro.ipc.NettyTransportCodec;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuan_fengyu on 2017/12/12 15:07.
 */
public class NettyClientSource extends AbstractSource implements EventDrivenSource, Configurable, NettyClientListener {

    private final Logger logger = LoggerFactory.getLogger(NettyClientSource.class);

    private final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss:SSS").create();

    private String host;

    private int port;

    private String user;

    private NettyClient nettyClient;

    private SourceCounter sourceCounter;

    @Override
    public void configure(Context context) {
        host = context.getString("host", "localhost");
        port = context.getInteger("port", 9090);
        user = context.getString("user", "user_default");
    }

    @Override
    public synchronized void start() {
        nettyClient = new NettyClient(host, port, user, this);
        sourceCounter = new SourceCounter(this.getName());
        sourceCounter.start();
        super.start();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMessage(NettyTransportCodec.NettyDataPack dataPack) {
        List<Event> events = new ArrayList<>();
        for (ByteBuffer buffer : dataPack.getDatas()) {
            events.add(HeaderAndBodySerialization.deserialize(buffer));
        }
        appendBatch(events);
    }

    public Status appendBatch(List<Event> events) {
        this.sourceCounter.incrementAppendBatchReceivedCount();
        this.sourceCounter.addToEventReceivedCount((long)events.size());

        try {
            this.getChannelProcessor().processEventBatch(events);
        } catch (Throwable var6) {
            logger.error("appendBatch fail", var6);
            return Status.FAILED;
        }

        this.sourceCounter.incrementAppendBatchAcceptedCount();
        this.sourceCounter.addToEventAcceptedCount((long)events.size());
        return Status.OK;
    }

    public void stop() {
        nettyClient.shutdown();

        this.sourceCounter.stop();

        super.stop();
    }

}
