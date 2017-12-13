package com.xiyuan.flume.source;

import org.apache.avro.ipc.NettyTransportCodec;

/**
 * Created by xiyuan_fengyu on 2017/12/12 19:17.
 */
public interface NettyClientListener {

    void onMessage(NettyTransportCodec.NettyDataPack dataPack);

}
