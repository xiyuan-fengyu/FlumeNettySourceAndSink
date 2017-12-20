package com.xiyuan.flume.sink;

import com.xiyuan.flume.serialization.HeaderAndBodySerialization;
import org.apache.avro.ipc.NettyTransportCodec;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * Created by xiyuan_fengyu on 2017/12/12 15:13.
 */
public class NettyServer {

    private final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private String host;

    private int port;

    private HashSet<String> users;

    private final ChannelFactory channelFactory;

    private final ChannelGroup channelGroup;

    private final HashMap<ChannelHandlerContext, Boolean> clients = new HashMap<>();

    public NettyServer(String host, int port, String users) {
        this.host = host;
        this.port = port;
        this.users = new HashSet<>(Arrays.asList(users.split(",")));

        channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                5
        );
        channelGroup = new DefaultChannelGroup();

        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setPipelineFactory(new MyChannelPipelineFactory());

        channelGroup.add(bootstrap.bind(new InetSocketAddress(host, port)));

        logger.info(String.format("NettyServer(%s:%d) started.", host, port));
    }

    public void broadcast(ByteBuffer buffer) {
        broadcast(Collections.singletonList(buffer));
    }

    public void broadcast(List<ByteBuffer> buffers) {
        try {
            NettyTransportCodec.NettyDataPack dataPack = new NettyTransportCodec.NettyDataPack();
            dataPack.setDatas(buffers);
            clients.forEach((client, auth) -> {
                if (auth) {
                    client.getChannel().write(dataPack);
//                    logger.info("send dataPack to" + client);
                }
            });
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        ChannelGroupFuture channelGroupFuture = channelGroup.close();
        channelGroupFuture.awaitUninterruptibly();
        channelFactory.releaseExternalResources();

        logger.info(String.format("NettyServer(%s:%d) started.", host, port));
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
            clients.put(ctx, false);
            logger.info(String.format("client connected: %s", e));
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelDisconnected(ctx, e);
            clients.remove(ctx);
            logger.info(String.format("client disconnected: %s", e));
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            NettyTransportCodec.NettyDataPack dataPack = (NettyTransportCodec.NettyDataPack) e.getMessage();
            for (ByteBuffer buffer : dataPack.getDatas()) {
                String msg = new String(buffer.array(), StandardCharsets.UTF_8);
                if (msg.startsWith("auth:")) {
                    if (users.contains(msg.substring(5))) {
                        clients.put(ctx, true);
                        logger.info(String.format("client auth success: %s", e));
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            logger.error("exceptionCaught", e.getCause());
            e.getChannel().close();
        }
    }

    public static void main(String[] args) {
        NettyServer nettyServer = new NettyServer("0.0.0.0", 9090, "user_default");
        Scanner scanner = new Scanner(System.in);
        String line;
        while ((line = scanner.nextLine()) != null && !line.equals("quit")) {
            Map<String, String> header = new HashMap<>();
            header.put("time", "" + new Date().getTime());
            Event event = EventBuilder.withBody(line, StandardCharsets.UTF_8, header);
            nettyServer.broadcast(HeaderAndBodySerialization.serialize(event));
        }
        nettyServer.shutdown();
    }

}
