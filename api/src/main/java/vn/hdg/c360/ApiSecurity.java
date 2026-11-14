package vn.hdg.c360;

import java.util.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.time.*;
import java.sql.Array;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;
import org.springframework.dao.DataAccessException;

@Component
public class ApiSecurity extends OncePerRequestFilter {
    private final C360Repository repository;private final AuditLog audit;private final ObjectMapper json;
    public ApiSecurity(C360Repository repository,AuditLog audit,ObjectMapper json){this.repository=repository;this.audit=audit;this.json=json;}
    static String route(HttpServletRequest r) {
        String p=org.springframework.web.util.UriUtils.decode(r.getRequestURI().substring(r.getContextPath().length()),StandardCharsets.UTF_8),method=r.getMethod(),base="/api/v1/ca-nhan/";
        if(method.equals("HEAD"))method="GET";
        if(method.equals("OPTIONS"))method=p.equals(base+"tra-cuu-lo")?"POST":"GET";
        if(method.equals("GET") && (p.equals(base+"tim-kiem")||p.equals(base+"thay-doi")))return p;
        if(method.equals("POST")&&p.equals(base+"tra-cuu-lo"))return p;
        if(method.equals("GET")&&p.startsWith(base)&&!p.substring(base.length()).isEmpty()&&!p.substring(base.length()).contains("/"))return base+"<ma>";
        return "UNMATCHED";
    }
    static String scope(String route) {return switch(route){case "/api/v1/ca-nhan/<ma>"->"c360:lookup";case "/api/v1/ca-nhan/tra-cuu-lo"->"c360:batch";case "/api/v1/ca-nhan/tim-kiem"->"c360:search";case "/api/v1/ca-nhan/thay-doi"->"c360:changes";default->null;};}
    static String hash(String value) {try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException();}}
    void error(ContentCachingResponseWrapper response,int status,String code) throws java.io.IOException {
        response.resetBuffer();response.setStatus(status);response.setContentType("application/json");json.writeValue(response.getOutputStream(),C360Service.map("error",code));
    }
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse raw,FilterChain chain) throws java.io.IOException,ServletException {
        long start=System.nanoTime();var observed=OffsetDateTime.now(ZoneOffset.UTC);var id=UUID.randomUUID();String client=null;String route=route(request);
        var response=new ContentCachingResponseWrapper(raw);
        try {
            if(audit.failed())throw new C360Service.Failure(503,"AUDIT_UNAVAILABLE");
            String key=request.getHeader("X-API-Key");
            if(key==null||C360Service.length(key)<32||C360Service.length(key)>256)throw new C360Service.Failure(401,"API_KEY_REQUIRED_OR_INVALID");
            String digest=hash(key);var keys=repository.key(digest);
            String saved=keys.isEmpty()?"0".repeat(64):(String)keys.getFirst().get("key_hash");
            boolean same=MessageDigest.isEqual(saved.getBytes(StandardCharsets.US_ASCII),digest.getBytes(StandardCharsets.US_ASCII));
            if(!same||keys.isEmpty())throw new C360Service.Failure(401,"API_KEY_REQUIRED_OR_INVALID");
            var row=keys.getFirst();client=(String)row.get("client_id");
            var scopes=Arrays.asList((String[])((Array)row.get("scopes")).getArray());
            if(scope(route)!=null&&!scopes.contains(scope(route)))throw new C360Service.Failure(403,"INSUFFICIENT_SCOPE");
            request.setAttribute("clientId",client);
            if(request.getContentLengthLong()>65536)throw new C360Service.Failure(413,"HTTP_413");
            HttpServletRequest routed=request;
            // Flask giữ ; trong mã, MVC mặc định coi đó là matrix parameter. Escape trước khi MVC match.
            if(request.getRequestURI().contains(";"))routed=new HttpServletRequestWrapper(request) {
                @Override public String getRequestURI(){return super.getRequestURI().replace(";","%3B");}
            };
            chain.doFilter(routed,response);
        }catch(C360Service.Failure e){error(response,e.status,e.code);}
        catch(DataAccessException e){error(response,503,"DATABASE_UNAVAILABLE");}
        catch(Exception e){error(response,500,"INTERNAL_ERROR");}
        var body=json.readTree(response.getContentAsByteArray());
        String result=body==null?"OK":body.path("status").asText(body.path("error").asText("OK"));
        var ms=BigDecimal.valueOf((System.nanoTime()-start)/1_000_000.0).setScale(3,java.math.RoundingMode.HALF_UP);
        if(!audit.offer(new AuditLog.Entry(id,observed,route,response.getStatus(),ms,result,client)))error(response,503,"AUDIT_BACKPRESSURE");
        response.setHeader("X-Request-ID",id.toString());response.setHeader("Cache-Control","no-store");response.copyBodyToResponse();
    }
}
