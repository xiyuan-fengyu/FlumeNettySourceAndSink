package com.xiyuan.flume.source;

import com.xiyuan.flume.serialization.HeaderAndBodySerialization;
import org.apache.avro.ipc.NettyTransportCodec;
import org.apache.flume.Event;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    private final AtomicBoolean isConnecting = new AtomicBoolean(false);

    private int unRecPongNum = 0;

    private static final int maxUnRecPongNum = 3;

    private static final long reconnectDelay = 5000;

    private static final long maxNoReadTime = 10000;

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
        logger.info("NettyClient started.");

        reconnect(0);
    }

    private void reconnect(long delay) {
        synchronized (isConnecting) {
            if (isConnecting.get() || !isWorking) {
                return;
            }
            isConnecting.set(true);

            if (delay > 0) {
                logger.info("NettyClient will try reconnect after " + (delay/ 1000) + " seconds");
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            logger.info("NettyClient reconnect");
            ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(host, port));
            channelFuture.addListener(future -> {
                isConnecting.set(false);
                if (!future.isSuccess()) {
                    reconnect(reconnectDelay);
                }
            });
        }
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
            channelPipeline.addLast("idle", new IdleStateHandler(new HashedWheelTimer(), maxNoReadTime, 0, 0, TimeUnit.MILLISECONDS));
            channelPipeline.addLast("handler", new MyChannelHandler());
            return channelPipeline;
        }
    }

    private class MyChannelHandler extends SimpleChannelHandler {

        private final String ping = "__PING__";

        private final String pong = "__PONG__";

        private final int pongBytesLen = pong.getBytes(StandardCharsets.UTF_8).length;

        private NettyTransportCodec.NettyDataPack pingMsg = new NettyTransportCodec.NettyDataPack();

        private MyChannelHandler() {
            pingMsg.setDatas(Collections.singletonList(ByteBuffer.wrap(ping.getBytes(StandardCharsets.UTF_8))));
        }

        @Override
        public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof IdleStateEvent) {
                if (((IdleStateEvent) e).getState() == IdleState.READER_IDLE) {
                    // 超过 maxNoReadTime 毫秒没有接收到服务端的消息，则尝试发送一个 ping 消息，服务端收到后会返回一个 pong 消息
                    // 如果连续 maxUnRecPongNum 次接收服务端消息超时，则视为连接已断开，主动断开通道，触发重连
                    if (unRecPongNum++ < maxUnRecPongNum) {
                        ctx.getChannel().write(pingMsg);
                    }
                    else {
                        ctx.getChannel().close();
                    }
                }
            }
            else {
                super.handleUpstream(ctx, e);
            }
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelConnected(ctx, e);
            logger.info("NettyClient connected.");
            send(ctx, "auth:" + user);
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelDisconnected(ctx, e);
            logger.info("NettyClient disconnected.");

            //重连
            reconnect(5000);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            e.getChannel().close();
            logger.error("exceptionCaught", e.getCause());
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            NettyTransportCodec.NettyDataPack dataPack = (NettyTransportCodec.NettyDataPack) e.getMessage();

            // 监测是否为 pong 消息
            if (dataPack.getDatas().size() == 1) {
                byte[] bytes = dataPack.getDatas().get(0).array();
                if (bytes.length == pongBytesLen) {
                    String msg = new String(bytes, StandardCharsets.UTF_8);
                    if (pong.equals(msg)) {
                        unRecPongNum = 0;
                        return;
                    }
                }
            }

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
