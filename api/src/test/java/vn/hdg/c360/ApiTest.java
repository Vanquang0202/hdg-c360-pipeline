package vn.hdg.c360;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.*;
import java.time.*;
import java.sql.Array;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ApiTest {
    // Giữ fixture outbox để kiểm tra lại, tránh Windows scanner khóa file lúc JUnit xóa temp.
    @TempDir(cleanup=org.junit.jupiter.api.io.CleanupMode.NEVER) Path folder;
    ObjectMapper json=new ObjectMapper().findAndRegisterModules();
    C360Repository repo;AuditLog audit;CursorCodec cursor;MockMvc http;
    String key="unit-key-"+UUID.randomUUID(),secret=UUID.randomUUID().toString();
    List<AuditLog.Entry> written=new ArrayList<>();
    @BeforeEach void setup() throws Exception {
        repo=mock(C360Repository.class);audit=new AuditLog(folder,json,written::addAll);cursor=new CursorCodec(secret,json);
        var scopes=mock(Array.class);when(scopes.getArray()).thenReturn(new String[]{"c360:lookup","c360:batch","c360:search","c360:changes"});
        when(repo.key(ApiSecurity.hash(key))).thenReturn(List.of(C360Service.map("key_hash",ApiSecurity.hash(key),"client_id","unit","scopes",scopes)));
        var service=new C360Service(repo,cursor);
        http=MockMvcBuilders.standaloneSetup(new C360Controller(service,json)).setControllerAdvice(new C360Controller.Errors()).addFilters(new ApiSecurity(repo,audit,json)).build();
    }
    @AfterEach void close() throws Exception{audit.close();}
    Map<String,Object> row(String code){return C360Service.map("ma_so_bhxh",code,"ho_ten","Nguyễn Văn An");}
    @Test void codeValidation(){for(Object v:Arrays.asList("",null,12," A","A".repeat(31),"A\n"))assertThrows(C360Service.Failure.class,()->C360Service.code(v));assertEquals("001",C360Service.code("001"));}
    @Test void unicodeName(){assertEquals("DO THI ANH",C360Service.name(" Đỗ\u00a0 thị  Ánh "));assertThrows(C360Service.Failure.class,()->C360Service.name(" "));}
    @Test void calendar(){for(String d:List.of("1995","199508","19950815","20000229"))C360Service.birth(d);for(String d:List.of("19950230","199500","19950000","0000","19951301"))assertThrows(C360Service.Failure.class,()->C360Service.birth(d));}
    @Test void cursorSignedAndClientBound(){var c=new CursorCodec.Cursor(OffsetDateTime.parse("2026-01-01T00:00:00+00:00"),1,OffsetDateTime.parse("2026-01-02T00:00:00+00:00"));String encoded=cursor.encode(c,"unit");assertEquals(c,cursor.decode(encoded,"unit"));assertThrows(C360Service.Failure.class,()->cursor.decode(encoded+"x","unit"));assertThrows(C360Service.Failure.class,()->cursor.decode(encoded,"other"));}
    @Test void invalidCursor(){assertThrows(C360Service.Failure.class,()->cursor.decode("bad","unit"));}
    @Test void missingKey() throws Exception{http.perform(get("/api/v1/ca-nhan/001")).andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("API_KEY_REQUIRED_OR_INVALID"));}
    @Test void wrongKey() throws Exception{http.perform(get("/api/v1/ca-nhan/001").header("X-API-Key",UUID.randomUUID().toString())).andExpect(status().isUnauthorized());}
    @Test void badStoredDigestConstantTimeCheck() throws Exception{when(repo.key(ApiSecurity.hash(key))).thenReturn(List.of(C360Service.map("key_hash","0".repeat(64))));http.perform(get("/api/v1/ca-nhan/001").header("X-API-Key",key)).andExpect(status().isUnauthorized());}
    @Test void scopeDenied() throws Exception{var a=mock(Array.class);when(a.getArray()).thenReturn(new String[]{"c360:lookup"});when(repo.key(ApiSecurity.hash(key))).thenReturn(List.of(C360Service.map("key_hash",ApiSecurity.hash(key),"client_id","unit","scopes",a)));http.perform(get("/api/v1/ca-nhan/thay-doi").header("X-API-Key",key)).andExpect(status().isForbidden());}
    @Test void currentPriority() throws Exception{when(repo.lookup("ma_so_bhxh","001")).thenReturn(List.of(row("001")));http.perform(get("/api/v1/ca-nhan/001").header("X-API-Key",key)).andExpect(status().isOk()).andExpect(jsonPath("$.data.maSoBhxh").value("001"));verify(repo,never()).lookup("ma_so_bhxh_cu","001");}
    @Test void oldCodeFallback() throws Exception{when(repo.lookup("ma_so_bhxh_cu","OLD")).thenReturn(List.of(row("001")));http.perform(get("/api/v1/ca-nhan/OLD").header("X-API-Key",key)).andExpect(status().isOk());}
    @Test void identityFallback() throws Exception{when(repo.lookup("so_dinh_danh","001000000001")).thenReturn(List.of(row("001")));http.perform(get("/api/v1/ca-nhan/001000000001").header("X-API-Key",key)).andExpect(status().isOk());}
    @Test void notFound() throws Exception{http.perform(get("/api/v1/ca-nhan/NONE").header("X-API-Key",key)).andExpect(status().isNotFound());}
    @Test void ambiguousOldCode() throws Exception{when(repo.lookup("ma_so_bhxh_cu","OLD")).thenReturn(List.of(row("001"),row("002")));http.perform(get("/api/v1/ca-nhan/OLD").header("X-API-Key",key)).andExpect(status().isConflict()).andExpect(jsonPath("$.candidates.length()").value(2));}
    @Test void ambiguousIdentity() throws Exception{when(repo.lookup("so_dinh_danh","001000000001")).thenReturn(List.of(row("001"),row("002")));http.perform(get("/api/v1/ca-nhan/001000000001").header("X-API-Key",key)).andExpect(status().isConflict());}
    @Test void invalidCodeHttp() throws Exception{http.perform(get("/api/v1/ca-nhan/"+"A".repeat(31)).header("X-API-Key",key)).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("INVALID_CODE"));}
    @Test void batchOrderAndDuplicates() throws Exception{when(repo.lookup("ma_so_bhxh","001")).thenReturn(List.of(row("001")));http.perform(post("/api/v1/ca-nhan/tra-cuu-lo").header("X-API-Key",key).contentType("application/json").content("{\"codes\":[\"001\",\"NONE\",\"001\"]}")).andExpect(status().isOk()).andExpect(jsonPath("$.results[0].status").value("FOUND")).andExpect(jsonPath("$.results[1].status").value("NOT_FOUND")).andExpect(jsonPath("$.results[2].status").value("FOUND"));verify(repo,times(1)).lookup("ma_so_bhxh","001");}
    @Test void badBatch() throws Exception{for(String body:List.of("{}","{\"codes\":[]}","{\"codes\":[12]}","bad"))http.perform(post("/api/v1/ca-nhan/tra-cuu-lo").header("X-API-Key",key).contentType("application/json").content(body)).andExpect(status().isBadRequest());}
    @Test void batchLimit(){assertThrows(C360Service.Failure.class,()->new C360Service(repo,cursor).batch(Collections.nCopies(1001,"001")));}
    @Test void searchNormalizationAndPartialDate() throws Exception{when(repo.search("NGUYEN VAN AN",List.of("1995","199508","19950815"),"19950815%",101)).thenReturn(List.of(row("001")));http.perform(get("/api/v1/ca-nhan/tim-kiem").header("X-API-Key",key).param("hoTen","Nguyễn Văn An").param("ngaySinh","19950815")).andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1));}
    @Test void invalidSearch() throws Exception{http.perform(get("/api/v1/ca-nhan/tim-kiem").header("X-API-Key",key).param("hoTen","An").param("ngaySinh","19950230")).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("INVALID_BIRTH_DATE"));}
    @Test void invalidLimit() throws Exception{http.perform(get("/api/v1/ca-nhan/thay-doi").header("X-API-Key",key).param("limit","0")).andExpect(status().isBadRequest());}
    @Test void badCursorHttp() throws Exception{http.perform(get("/api/v1/ca-nhan/thay-doi").header("X-API-Key",key).param("cursor","bad")).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("INVALID_CURSOR"));}
    @Test void bestEffortSemantics() throws Exception{http.perform(get("/api/v1/ca-nhan/thay-doi").header("X-API-Key",key)).andExpect(status().isOk()).andExpect(jsonPath("$.consistency").value("BEST_EFFORT_CURRENT_ROWS")).andExpect(jsonPath("$.limitations.length()").value(2));}
    @Test void auditContainsNoPiiOrKey() throws Exception{http.perform(get("/api/v1/ca-nhan/PRIVATE-CODE").header("X-API-Key",key)).andExpect(header().exists("X-Request-ID")).andExpect(header().string("Cache-Control","no-store"));try(var files=Files.list(folder)){String content=Files.readString(files.filter(p->p.toString().endsWith(".json")).findFirst().orElseThrow());assertFalse(content.contains(key));assertFalse(content.contains("PRIVATE-CODE"));assertTrue(content.contains("/api/v1/ca-nhan/<ma>"));}}
    @Test void databaseFailure() throws Exception{when(repo.key(any())).thenThrow(new org.springframework.dao.DataAccessResourceFailureException("injected"));http.perform(get("/api/v1/ca-nhan/001").header("X-API-Key",key)).andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.error").value("DATABASE_UNAVAILABLE"));}
    @Test void encodedRouteUsesDecodedScope(){var r=new org.springframework.mock.web.MockHttpServletRequest("GET","/api/v1/ca-nhan/%74im-kiem");assertEquals("c360:search",ApiSecurity.scope(ApiSecurity.route(r)));}
    @Test void headAndOptionsKeepScope(){for(String method:List.of("HEAD","OPTIONS")){var r=new org.springframework.mock.web.MockHttpServletRequest(method,"/api/v1/ca-nhan/thay-doi");assertEquals("c360:changes",ApiSecurity.scope(ApiSecurity.route(r)));}}
    @Test void semicolonCodeIsNotStaticSearch(){var r=new org.springframework.mock.web.MockHttpServletRequest("GET","/api/v1/ca-nhan/tim-kiem;x=1");assertEquals("/api/v1/ca-nhan/<ma>",ApiSecurity.route(r));}
    @Test void auditPersistenceFailureRejectsResponse() throws Exception{var unavailable=mock(AuditLog.class);when(unavailable.offer(any())).thenReturn(false);var mvc=MockMvcBuilders.standaloneSetup(new C360Controller(new C360Service(repo,cursor),json)).setControllerAdvice(new C360Controller.Errors()).addFilters(new ApiSecurity(repo,unavailable,json)).build();mvc.perform(get("/api/v1/ca-nhan/PRIVATE")).andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.error").value("AUDIT_BACKPRESSURE"));}
    @Test void auditDatabaseFailureIsVisible() throws Exception{var unavailable=mock(AuditLog.class);when(unavailable.failed()).thenReturn(true);when(unavailable.offer(any())).thenReturn(true);var mvc=MockMvcBuilders.standaloneSetup(new C360Controller(new C360Service(repo,cursor),json)).setControllerAdvice(new C360Controller.Errors()).addFilters(new ApiSecurity(repo,unavailable,json)).build();mvc.perform(get("/api/v1/ca-nhan/PRIVATE").header("X-API-Key",key)).andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.error").value("AUDIT_UNAVAILABLE"));verify(repo,never()).key(any());}

    @Test void semicolonIsDataInLookupHttp() throws Exception{when(repo.lookup("ma_so_bhxh","001")).thenReturn(List.of(row("001")));http.perform(get("/api/v1/ca-nhan/001;v=1").header("X-API-Key",key)).andExpect(status().isNotFound());verify(repo).lookup("ma_so_bhxh","001;v=1");}

}
