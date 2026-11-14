package vn.hdg.c360;

import com.fasterxml.jackson.databind.*;
import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import java.nio.charset.StandardCharsets;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.dao.DataAccessException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import org.springframework.web.HttpRequestMethodNotSupportedException;

@RestController
public class C360Controller {
    private final C360Service service;private final ObjectMapper json;
    public C360Controller(C360Service service,ObjectMapper json){this.service=service;this.json=json;}
    @GetMapping("/api/v1/ca-nhan/{ma}")
    public ResponseEntity<?> lookup(@PathVariable String ma) {
        var result=service.single(ma);
        int status=switch((String)result.get("status")){case "FOUND"->200;case "NOT_FOUND"->404;default->409;};
        return ResponseEntity.status(status).body(result);
    }
    @PostMapping("/api/v1/ca-nhan/tra-cuu-lo")
    public Object batch(@RequestBody(required=false) String body,HttpServletRequest request) {
        if(body!=null && body.getBytes(StandardCharsets.UTF_8).length>65536)throw new C360Service.Failure(413,"HTTP_413");
        try {
            String content=request.getContentType();
            if(content==null||!(content.split(";")[0].equalsIgnoreCase("application/json")||content.split(";")[0].toLowerCase(Locale.ROOT).matches("application/.*\\+json")))throw new IllegalArgumentException();
            JsonNode node=json.readTree(body);
            if(node==null||!node.isObject()||node.size()!=1||!node.has("codes")||!node.get("codes").isArray())throw new IllegalArgumentException();
            var codes=new ArrayList<Object>();for(JsonNode item:node.get("codes"))codes.add(item.isTextual()?item.textValue():null);
            return service.batch(codes);
        }catch(C360Service.Failure e){throw e;}
        catch(Exception e){throw new C360Service.Failure(400,"INVALID_BATCH_1_TO_1000");}
    }
    @GetMapping("/api/v1/ca-nhan/tim-kiem")
    public Object search(@RequestParam(required=false) String hoTen,@RequestParam(required=false) String ngaySinh,@RequestParam(required=false) String limit) {return service.search(hoTen,ngaySinh,limit);}
    @GetMapping("/api/v1/ca-nhan/thay-doi")
    public Object changes(@RequestParam(required=false) String cursor,@RequestParam(required=false) String limit,HttpServletRequest request) {return service.changes(cursor,limit,(String)request.getAttribute("clientId"));}

    @RestControllerAdvice
    static class Errors {
        @ExceptionHandler(C360Service.Failure.class) ResponseEntity<?> invalid(C360Service.Failure e){return ResponseEntity.status(e.status).body(C360Service.map("error",e.code));}
        @ExceptionHandler(DataAccessException.class) ResponseEntity<?> database(DataAccessException e){return ResponseEntity.status(503).body(C360Service.map("error","DATABASE_UNAVAILABLE"));}
        @ExceptionHandler(NoResourceFoundException.class) ResponseEntity<?> missing(Exception e){return ResponseEntity.status(404).body(C360Service.map("error","HTTP_404"));}
        @ExceptionHandler(HttpRequestMethodNotSupportedException.class) ResponseEntity<?> method(Exception e){return ResponseEntity.status(405).body(C360Service.map("error","HTTP_405"));}
        @ExceptionHandler(Exception.class) ResponseEntity<?> unexpected(Exception e){return ResponseEntity.status(500).body(C360Service.map("error","INTERNAL_ERROR"));}
    }
}
