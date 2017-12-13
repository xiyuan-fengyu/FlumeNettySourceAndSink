package com.xiyuan.flume.source;

import com.xiyuan.flume.serialization.HeaderAndBodySerialization;
import org.apache.avro.ipc.NettyTransportCodec;
import org.apache.flume.Event;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by xiyuan_fengyu on 2017/12/12 15:42.
 */
public class NettyClient {

    private final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private String host;

    private int port;

    private String user;

    private final ChannelFactory channelFactory;

    private final ClientBootstrap bootstrap;

    private boolean isWorking = true;

    private AtomicBoolean isConnecting = new AtomicBoolean(false);

    private AtomicBoolean isConnected = new AtomicBoolean(false);

    private NettyClientListener listener;

    public NettyClient(String host, int port, String user, NettyClientListener listener) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.listener = listener;

        channelFactory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                5
        );

        bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setPipelineFactory(new MyChannelPipelineFactory());
        reconnect(0);

        logger.info("NettyClient started.");
    }

    private void reconnect(long delay) {
//        logger.info(String.format("isConnecting=%b, isConnected=%b, isWorking=%b", isConnected.get(), isConnected.get(), isWorking));
        if (isConnecting.get() || isConnected.get() || !isWorking) {
            return;
        }

        isConnecting.set(true);
        if (delay > 0) {
            try {
                logger.info(String.format("reconnect after %d ms.", delay));
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        bootstrap.connect(new InetSocketAddress(host, port));
    }

    public void send(ChannelHandlerContext ctx, String msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8));
        send(ctx, Collections.singletonList(buffer));
    }

    public void send(ChannelHandlerContext ctx, List<ByteBuffer> buffers) {
        try {
            NettyTransportCodec.NettyDataPack dataPack = new NettyTransportCodec.NettyDataPack();
            dataPack.setDatas(buffers);
            ctx.getChannel().write(dataPack);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        isWorking = false;
        channelFactory.releaseExternalResources();

        logger.info("NettyClient stopped.");
    }

    private class MyChannelPipelineFactory implements ChannelPipelineFactory {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline channelPipeline = Channels.pipeline();
            channelPipeline.addLast("frameDecoder", new NettyTransportCodec.NettyFrameDecoder());
            channelPipeline.addLast("frameEncoder", new NettyTransportCodec.NettyFrameEncoder());
            channelPipeline.addLast("handler", new MyChannelHandler());
            return channelPipeline;
        }
    }

    private class MyChannelHandler extends SimpleChannelHandler {

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelConnected(ctx, e);

            send(ctx, "auth:" + user);

            isConnecting.set(false);
            isConnected.set(true);
            logger.info("NettyClient connected.");
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelDisconnected(ctx, e);

            logger.info("NettyClient disconnected.");

            isConnected.set(false);
            isConnecting.set(false);
            reconnect(5000);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            logger.error("exceptionCaught", e.getCause());
            e.getChannel().close();

            if (e.getCause() instanceof ConnectException) {
                isConnected.set(false);
                isConnecting.set(false);
                reconnect(5000);
            }
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            NettyTransportCodec.NettyDataPack dataPack = (NettyTransportCodec.NettyDataPack) e.getMessage();
            if (listener == null) {
                for (ByteBuffer buffer : dataPack.getDatas()) {
                    Event event = HeaderAndBodySerialization.deserialize(buffer);
                    System.out.println("Header: " + event.getHeaders());
                    System.out.println("Body: " + new String(event.getBody(), StandardCharsets.UTF_8));
                    System.out.println();
                }
            }
            else {
                listener.onMessage(dataPack);
            }
        }

    }

    public static void main(String[] args) {
        NettyClient nettyClient = new NettyClient("localhost", 9090, "user_default", null);
        Scanner scanner = new Scanner(System.in);
        String line;
        while ((line = scanner.nextLine()) != null && !line.equals("quit")) {

        }
        nettyClient.shutdown();
    }

}
