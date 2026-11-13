package vn.hdg.c360;

import java.util.*;
import java.util.function.Consumer;
import java.nio.file.*;
import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

public class AuditLog implements AutoCloseable {
    public record Entry(UUID requestId,OffsetDateTime observedAt,String endpoint,int httpStatus,
                        BigDecimal elapsedMs,String resultStatus,String clientId) {}
    private final Path directory;private final ObjectMapper json;private final Consumer<List<Entry>> writer;
    private final FileChannel channel;private final FileLock lock;
    private volatile boolean failed;private final Object flushLock=new Object();private volatile boolean closed;
    public AuditLog(Path directory,ObjectMapper json,Consumer<List<Entry>> writer) throws Exception {this(directory,json,writer,"unit");}
    public AuditLog(Path directory,ObjectMapper json,Consumer<List<Entry>> writer,String target) throws Exception {
        this.directory=directory;this.json=json;this.writer=writer;Files.createDirectories(directory);
        channel=FileChannel.open(directory.resolve(".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        try {lock=channel.tryLock();if(lock==null)throw new IllegalStateException("AUDIT_OUTBOX_ALREADY_IN_USE");
            Path marker=directory.resolve("target.sha256");String fingerprint=ApiSecurity.hash(target);
            if(Files.exists(marker) && !Files.readString(marker).equals(fingerprint))throw new IllegalStateException("AUDIT_OUTBOX_WRONG_TARGET");
            if(!Files.exists(marker)) {
                try(var files=Files.list(directory)){if(files.anyMatch(p->p.toString().endsWith(".json")))throw new IllegalStateException("AUDIT_OUTBOX_UNBOUND");}
                Files.writeString(marker,fingerprint,StandardOpenOption.CREATE_NEW);
            }
        }
        catch(Exception e){channel.close();throw e;}
    }
    public boolean failed(){return failed;}
    public synchronized boolean offer(Entry entry) {
        // Ghi fsync trước response. Không PII/key/body/query; mỗi request một UUID ổn định để replay.
        try {
            if(closed)return false;
            try(var files=Files.list(directory)){if(files.filter(p->p.toString().endsWith(".json")).count()>=2000)return false;}
            Path temp=directory.resolve(entry.requestId()+".tmp"),dest=directory.resolve(entry.requestId()+".json");
            try(var file=FileChannel.open(temp,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {
                var bytes=ByteBuffer.wrap(json.writeValueAsBytes(entry));while(bytes.hasRemaining())file.write(bytes);file.force(true);
            }
            Files.move(temp,dest,StandardCopyOption.ATOMIC_MOVE);return true;
        }catch(Exception e){LoggerFactory.getLogger(getClass()).error("AUDIT_DURABLE_WRITE_FAILED: {}",e.getClass().getSimpleName());return false;}
    }
    @Scheduled(fixedDelayString="${c360.audit.delay:100}",initialDelayString="${c360.audit.initial-delay:0}")
    public void flush() {
        synchronized(flushLock) {
        if(closed)return;
        try {
            List<Path> paths;
            try(var stream=Files.list(directory)){paths=stream.filter(p->p.toString().endsWith(".json")).sorted().limit(100).toList();}
            if(paths.isEmpty()){failed=false;return;}
            var entries=new ArrayList<Entry>();for(Path p:paths)entries.add(json.readValue(Files.readAllBytes(p),Entry.class));
            writer.accept(entries); // Callback có transaction PostgreSQL; lỗi commit giữ nguyên file.
            for(Path p:paths)Files.delete(p);
            failed=false;
        }catch(Exception e){failed=true;LoggerFactory.getLogger(getClass()).warn("AUDIT_RETRY_PENDING: {}",e.getClass().getSimpleName());}
    }
    }
    public synchronized long pending() throws Exception {try(var stream=Files.list(directory)){return stream.filter(p->p.toString().endsWith(".json")).count();}}
    public void close() throws Exception {
        synchronized(flushLock) {
        long end=System.nanoTime()+10_000_000_000L;
        while(pending()>0 && System.nanoTime()<end){flush();if(failed)break;}
        if(pending()>0)LoggerFactory.getLogger(getClass()).warn("AUDIT_PENDING_ON_DISK: {}",pending());
        closed=true;lock.release();channel.close();
        }
    }
}
