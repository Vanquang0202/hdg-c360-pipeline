package vn.hdg.c360;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import java.time.*;
import java.math.BigDecimal;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.*;

class AuditTest {
    // Giữ fixture outbox để kiểm tra lại, tránh Windows scanner khóa file lúc JUnit xóa temp.
    @TempDir(cleanup=org.junit.jupiter.api.io.CleanupMode.NEVER) Path folder;
    ObjectMapper json=new ObjectMapper().findAndRegisterModules();
    AuditLog.Entry entry(){return new AuditLog.Entry(UUID.randomUUID(),OffsetDateTime.now(ZoneOffset.UTC),"/api/v1/ca-nhan/<ma>",200,BigDecimal.ONE,"FOUND","unit");}
    @Test void durableBeforeDatabaseCommitAndRestart() throws Exception {
        var first=new AuditLog(folder,json,e->{throw new IllegalStateException("injected");});var record=entry();assertTrue(first.offer(record));first.flush();assertTrue(first.failed());assertEquals(1,first.pending());first.close();
        var saved=new ArrayList<AuditLog.Entry>();try(var second=new AuditLog(folder,json,saved::addAll)){second.flush();assertEquals(0,second.pending());assertEquals(record.requestId(),saved.getFirst().requestId());}
    }
    @Test void rejectsDifferentDatabaseTarget() throws Exception {try(var first=new AuditLog(folder,json,e->{},"db-one")){} assertThrows(IllegalStateException.class,()->new AuditLog(folder,json,e->{},"db-two"));}
    @Test void retryRetainsFiles() throws Exception{int[] count={0};try(var audit=new AuditLog(folder,json,e->{if(count[0]++==0)throw new IllegalStateException();})){assertTrue(audit.offer(entry()));audit.flush();assertEquals(1,audit.pending());audit.flush();assertEquals(0,audit.pending());}}
    @Test void exclusiveOutboxLock() throws Exception{try(var first=new AuditLog(folder,json,e->{})){assertThrows(Exception.class,()->new AuditLog(folder,json,e->{}));}}
    @Test void corruptRecordNotSilentlyDeleted() throws Exception{try(var audit=new AuditLog(folder,json,e->{})){Files.writeString(folder.resolve("bad.json"),"invalid");audit.flush();assertTrue(audit.failed());assertEquals(1,audit.pending());}}
    @Test void persistenceFailureIsVisible() throws Exception{var audit=new AuditLog(folder,json,e->{});audit.close();assertFalse(audit.offer(entry()));}
    @Test void unboundPendingIsNotReassigned() throws Exception{Files.writeString(folder.resolve("orphan.json"),"{}");assertThrows(IllegalStateException.class,()->new AuditLog(folder,json,e->{},"other-db"));}

}
