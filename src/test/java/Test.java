
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiyuan_fengyu on 2017/12/13 9:51.
 */
public class Test {

    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss:SSS").create();

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        Map<String, String> headerMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            headerMap.put("" + new Date().getTime() + "_" + (int) (Math.random() * 1000), "" + Math.random());
        }

        byte[] header = gson.toJson(headerMap).getBytes(StandardCharsets.UTF_8);
        byte[] body = {1, 2, 3};
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + header.length + body.length);
        buffer.putInt(header.length);
        buffer.put(header);
        buffer.put(body);
        buffer.flip();

        int headLen = buffer.getInt();
        byte[] newHeader = new byte[headLen];
        byte[] newBody = new byte[buffer.array().length - Integer.BYTES - headLen];
        buffer.get(newHeader);
        buffer.get(newBody);

        String json = new String(newHeader, StandardCharsets.UTF_8);

        System.out.println(String.format("headerLen=%d, newHeaderLen=%d", header.length, newHeader.length));
        System.out.println(json);
        Event event = EventBuilder.withBody(newBody, (Map<String, String>) gson.fromJson(json, Map.class));
        System.out.println(event);
    }

}