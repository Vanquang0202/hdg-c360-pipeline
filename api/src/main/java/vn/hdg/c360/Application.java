package vn.hdg.c360;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.net.URI;
import java.sql.*;
import javax.sql.DataSource;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootApplication
@EnableScheduling
public class Application {
    public static void main(String[] args) { SpringApplication.run(Application.class,args); }

    @Bean JdbcTransactionManager transactionManager(DataSource ds,Environment env) throws Exception {
        verify(ds,env);
        var tx=new JdbcTransactionManager(ds);
        tx.setEnforceReadOnly(true); // SELECT nghiệp vụ luôn có SET TRANSACTION READ ONLY.
        return tx;
    }

    static void verify(DataSource ds,Environment env) throws Exception {
            try(var c=ds.getConnection()) {
                var uri=URI.create(c.getMetaData().getURL().substring(5));
                boolean target="127.0.0.1".equals(uri.getHost()) &&
                  ((uri.getPort()==15432 && "/hdg_mirror".equals(uri.getPath())) ||
                   (uri.getPort()==15433 && uri.getPath().matches("/hdg_api_[0-9a-f]{8}_test")));
                if(!target || !"127.0.0.1".equals(env.getProperty("server.address")))
                    throw new IllegalStateException("API_LOCAL_DATABASE_GUARD");
                try(var st=c.createStatement(); var rs=st.executeQuery("SELECT has_table_privilege(current_user,'c360.c360_thong_tin_ca_nhan','SELECT'), has_table_privilege(current_user,'c360.c360_thong_tin_ca_nhan','INSERT,UPDATE,DELETE'), (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)")) {
                    rs.next();
                    if(!rs.getBoolean(1)||rs.getBoolean(2)||rs.getBoolean(3))
                        throw new IllegalStateException("API_LEAST_PRIVILEGE_GUARD");
                }
            }
            if(env.getRequiredProperty("c360.cursor-secret").length()<32)
                throw new IllegalStateException("CURSOR_SECRET_TOO_SHORT");
    }

    @Bean(destroyMethod="close") AuditLog auditLog(JdbcTemplate jdbc,JdbcTransactionManager manager,
                                                   ObjectMapper json,Environment env,DataSource ds) throws Exception {
        var tx=new TransactionTemplate(manager);tx.setTimeout(5);
        return new AuditLog(Path.of(env.getRequiredProperty("c360.audit.path")),json,entries ->
            tx.executeWithoutResult(status -> jdbc.batchUpdate("INSERT INTO c360_api.c360_api_audit(request_id,observed_at,endpoint,http_status,elapsed_ms,result_status,client_id) VALUES(?,?,?,?,?,?,?) ON CONFLICT(request_id) DO NOTHING",entries,100,
              (ps,e)->{ps.setObject(1,e.requestId());ps.setObject(2,e.observedAt());ps.setString(3,e.endpoint());ps.setInt(4,e.httpStatus());ps.setBigDecimal(5,e.elapsedMs());ps.setString(6,e.resultStatus());ps.setString(7,e.clientId());})),env.getRequiredProperty("spring.datasource.url")+"|"+env.getRequiredProperty("spring.datasource.username"));
    }
}
