package vn.hdg.c360;

import java.util.*;
import java.time.*;
import java.sql.Timestamp;
import java.text.Normalizer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
@Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ,timeout=5)
public class C360Service {
    private final C360Repository repository;private final CursorCodec cursor;
    public C360Service(C360Repository repository,CursorCodec cursor){this.repository=repository;this.cursor=cursor;}
    public static class Failure extends RuntimeException {
        final int status;final String code;
        public Failure(int status,String code){super(code);this.status=status;this.code=code;}
    }
    public static Map<String,Object> map(Object... pairs) {
        var result=new LinkedHashMap<String,Object>();
        for(int i=0;i<pairs.length;i+=2)result.put((String)pairs[i],pairs[i+1]);
        return result;
    }
    public static int length(String s){return s.codePointCount(0,s.length());}
    static boolean space(int c){return (c>=9&&c<=13)||(c>=28&&c<=32)||c==0x85||c==0xa0||c==0x1680||(c>=0x2000&&c<=0x200a)||c==0x2028||c==0x2029||c==0x202f||c==0x205f||c==0x3000;}
    public static String code(Object value) {
        if(!(value instanceof String s)||length(s)<1||length(s)>30||space(s.codePointAt(0))||space(s.codePointBefore(s.length()))||s.codePoints().anyMatch(c->c<32||c==127))
            throw new Failure(400,"INVALID_CODE");
        return s;
    }
    public static int limit(String value) {
        if(value==null)value="100";
        if(!value.matches("[0-9]{1,4}")||Integer.parseInt(value)<1||Integer.parseInt(value)>1000)throw new Failure(400,"INVALID_LIMIT");
        return Integer.parseInt(value);
    }
    public static String name(String value) {
        if(value==null||length(value)<1||length(value)>250||value.codePoints().anyMatch(c->c<32))throw new Failure(400,"INVALID_NAME");
        var words=new StringBuilder();boolean pending=false;
        for(int c:value.codePoints().toArray()) {
            if(space(c)){pending=words.length()>0;continue;}
            if(pending)words.append(' ');words.appendCodePoint(c);pending=false;
        }
        if(words.isEmpty())throw new Failure(400,"INVALID_NAME");
        String nfd=Normalizer.normalize(words.toString().replace('đ','d').replace('Đ','D'),Normalizer.Form.NFD);
        var out=new StringBuilder();nfd.codePoints().filter(c->Character.getType(c)!=Character.NON_SPACING_MARK).forEach(out::appendCodePoint);
        return out.toString().toUpperCase(Locale.ROOT);
    }
    public static void birth(String date) {
        try {
            if(date==null||!date.matches("[0-9]{4}|[0-9]{6}|[0-9]{8}"))throw new IllegalArgumentException();
            int y=Integer.parseInt(date.substring(0,4));if(y<1)throw new IllegalArgumentException();
            if(date.length()==6)YearMonth.of(y,Integer.parseInt(date.substring(4,6)));
            if(date.length()==8)LocalDate.of(y,Integer.parseInt(date.substring(4,6)),Integer.parseInt(date.substring(6,8)));
        }catch(RuntimeException e){throw new Failure(400,"INVALID_BIRTH_DATE");}
    }
    public Map<String,Object> single(String value) {return lookup(code(value));}
    private Map<String,Object> lookup(String value) {
        for(String column:List.of("ma_so_bhxh","ma_so_bhxh_cu","so_dinh_danh")) {
            var rows=repository.lookup(column,value);
            if(rows.size()==1)return map("status","FOUND","data",C360Repository.publicRow(rows.getFirst()));
            if(!rows.isEmpty())return map("status","CONFLICT","candidates",rows.stream().limit(100).map(r->map("maSoBhxh",r.get("ma_so_bhxh"))).toList(),"truncated",rows.size()>100);
        }
        return map("status","NOT_FOUND");
    }
    public Map<String,Object> batch(List<?> codes) {
        if(codes==null||codes.isEmpty()||codes.size()>1000)throw new Failure(400,"INVALID_BATCH_1_TO_1000");
        var values=codes.stream().map(C360Service::code).toList();
        var cache=new HashMap<String,Map<String,Object>>();var results=new ArrayList<Map<String,Object>>();
        for(String value:values){var item=map("code",value);item.putAll(cache.computeIfAbsent(value,this::lookup));results.add(item);}
        return map("status","OK","results",results);
    }
    public Map<String,Object> search(String hoTen,String ngaySinh,String limitValue) {
        String normalized=name(hoTen);birth(ngaySinh);int limit=limit(limitValue);
        var prefixes=new ArrayList<String>();for(int n:List.of(4,6,8))if(n<=ngaySinh.length())prefixes.add(ngaySinh.substring(0,n));
        var rows=repository.search(normalized,prefixes,ngaySinh+"%",limit+1);
        return map("status","OK","items",rows.stream().limit(limit).map(C360Repository::publicRow).toList(),"truncated",rows.size()>limit);
    }
    public Map<String,Object> changes(String value,String limitValue,String client) {
        int limit=limit(limitValue);
        // Snapshot REPEATABLE READ giữ control/retention và payload nhất quán trong một request.
        var state=repository.feedState();String feed=state.get("feed_id").toString();
        long high=((Number)state.get("last_id")).longValue(),floor=((Number)state.get("retained_after")).longValue();
        var c=value==null||value.isEmpty()?new CursorCodec.FeedCursor(feed,0,high):
            value.equals("now")?new CursorCodec.FeedCursor(feed,high,high):cursor.decodeFeed(value,client);
        if(!feed.equals(c.feed()))throw new Failure(410,"FEED_CHANGED_RESYNC_REQUIRED");
        if(c.after()<floor)throw new Failure(410,"CURSOR_EXPIRED_RESYNC_REQUIRED");
        if(c.until()>high)throw new Failure(409,"CURSOR_AHEAD_OF_FEED");
        long until=c.after()==c.until()?high:c.until();
        var rows=repository.feed(c.after(),until,limit+1);
        var items=new ArrayList<Map<String,Object>>();long last=c.after();
        for(var r:rows.stream().limit(limit).toList()) {
            @SuppressWarnings("unchecked") var payload=(Map<String,Object>)r.get("payload");
            var item=new LinkedHashMap<String,Object>(payload);
            last=((Number)r.get("event_id")).longValue();
            item.put("id",last);item.put("eventId",last);item.put("operation",r.get("operation"));
            item.put("transactionId",r.get("transaction_id"));
            item.put("syncDate",CursorCodec.pythonTime(time(r.get("occurred_at"))));items.add(item);
        }
        boolean more=rows.size()>limit;
        // Cursor luôn trả về, kể cả hết trang: client lưu để poll tiếp sau restart.
        if(!more)last=until;
        String next=cursor.encodeFeed(new CursorCodec.FeedCursor(feed,last,until),client);
        return map("status","OK","items",items,"nextCursor",next,"until",until,"hasMore",more,
            "feedId",feed,"retainedAfter",floor,"consistency","C360_COMMITTED_CHANGE_LOG","limitations",List.of("CLIENT_ACK_NOT_TRACKED"));
    }
    static OffsetDateTime time(Object value){return value instanceof OffsetDateTime d?d:((Timestamp)value).toInstant().atOffset(ZoneOffset.UTC);}
}
