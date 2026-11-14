package vn.hdg.c360;

import java.util.*;
import java.time.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CursorCodec {
    private final String secret;private final ObjectMapper json;
    public record Cursor(OffsetDateTime date,long id,OffsetDateTime until) {}
    public CursorCodec(@Value("${c360.cursor-secret}") String secret,ObjectMapper json){this.secret=secret;this.json=json;}
    byte[] sign(String text) {
        try {var mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8),"HmacSHA256"));return mac.doFinal(text.getBytes(StandardCharsets.UTF_8));}
        catch(Exception e){throw new IllegalStateException("CURSOR_SIGNATURE_FAILURE");}
    }
    public String encode(Cursor c,String client) {
        try {
            var data=C360Service.map("date",pythonTime(c.date()),"id",c.id(),"until",pythonTime(c.until()),"client",client);
            String payload=Base64.getUrlEncoder().withoutPadding().encodeToString(json.writeValueAsBytes(data));
            return payload+"."+HexFormat.of().formatHex(sign(payload));
        }catch(Exception e){throw new IllegalStateException("CURSOR_ENCODING_FAILURE");}
    }
    public Cursor decode(String value,String client) {
        try {
            if(value.length()>2048)throw new IllegalArgumentException();
            String[] parts=value.split("\\.",-1);
            if(parts.length!=2 || !MessageDigest.isEqual(sign(parts[0]),HexFormat.of().parseHex(parts[1])))throw new IllegalArgumentException();
            var n=json.readTree(Base64.getUrlDecoder().decode(parts[0]));
            if(n.size()!=4 || !n.has("date") || !n.has("id") || !n.has("until") || !n.has("client") ||
               !n.get("id").isIntegralNumber() || !n.get("id").canConvertToLong() || n.get("id").asLong()<0 ||
               !n.get("client").isTextual() || !client.equals(n.get("client").textValue()))throw new IllegalArgumentException();
            var c=new Cursor(OffsetDateTime.parse(n.get("date").textValue()),n.get("id").asLong(),OffsetDateTime.parse(n.get("until").textValue()));
            if(c.date().isAfter(c.until()))throw new IllegalArgumentException();
            return c;
        }catch(Exception e){throw new C360Service.Failure(400,"INVALID_CURSOR");}
    }
    public record FeedCursor(String feed,long after,long until) {}
    public String encodeFeed(FeedCursor c,String client) {
        try {
            var data=C360Service.map("v",2,"feed",c.feed(),"after",c.after(),"until",c.until(),"client",client);
            var payload=Base64.getUrlEncoder().withoutPadding().encodeToString(json.writeValueAsBytes(data));
            return payload+"."+HexFormat.of().formatHex(sign(payload));
        }catch(Exception e){throw new IllegalStateException("CURSOR_ENCODING_FAILURE");}
    }
    public FeedCursor decodeFeed(String value,String client) {
        try {
            if(value.length()>2048)throw new IllegalArgumentException();
            var parts=value.split("\\.",-1);
            if(parts.length!=2||!MessageDigest.isEqual(sign(parts[0]),HexFormat.of().parseHex(parts[1])))throw new IllegalArgumentException();
            var n=json.readTree(Base64.getUrlDecoder().decode(parts[0]));
            if(!n.has("v")||n.path("v").asInt()!=2)throw new C360Service.Failure(409,"CURSOR_VERSION_UNSUPPORTED");
            if(n.size()!=5||!client.equals(n.path("client").asText())||!n.path("after").isIntegralNumber()||!n.path("after").canConvertToLong()||!n.path("until").isIntegralNumber()||!n.path("until").canConvertToLong())throw new IllegalArgumentException();
            String feed=n.path("feed").asText();UUID.fromString(feed);
            long after=n.path("after").asLong(),until=n.path("until").asLong();
            if(after<0||until<after)throw new IllegalArgumentException();
            return new FeedCursor(feed,after,until);
        }catch(C360Service.Failure e){throw e;}
        catch(Exception e){throw new C360Service.Failure(400,"INVALID_CURSOR");}
    }
    public static String pythonTime(OffsetDateTime date) {
        String text=date.format(java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss"));
        if(date.getNano()!=0)text+="."+String.format(Locale.ROOT,"%06d",date.getNano()/1000);
        return text+(date.getOffset().equals(ZoneOffset.UTC)?"+00:00":date.getOffset().toString());
    }
}
