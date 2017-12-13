package com.xiyuan.flume.serialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Created by xiyuan_fengyu on 2017/12/13 11:27.
 */
public class HeaderAndBodySerialization {

    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss:SSS").create();

    public static ByteBuffer serialize(Event event) {
        byte[] header = gson.toJson(event.getHeaders()).getBytes(StandardCharsets.UTF_8);
        byte[] body = event.getBody();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + header.length + body.length);
        buffer.putInt(header.length);
        buffer.put(header);
        buffer.put(body);
        buffer.flip();
        return buffer;
    }

    @SuppressWarnings("unchecked")
    public static Event deserialize(ByteBuffer buffer) {
        int headLen = buffer.getInt();
        byte[] header = new byte[headLen];
        byte[] body = new byte[buffer.array().length - Integer.BYTES - headLen];
        buffer.get(header);
        buffer.get(body);
        return EventBuilder.withBody(body, (Map<String, String>) gson.fromJson(new String(header, StandardCharsets.UTF_8), Map.class));
    }

}
