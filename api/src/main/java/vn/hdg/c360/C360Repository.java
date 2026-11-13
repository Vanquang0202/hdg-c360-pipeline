package vn.hdg.c360;

import java.util.*;
import java.time.OffsetDateTime;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.*;

@Repository
public class C360Repository {
    static final String TABLE="c360.c360_thong_tin_ca_nhan";
    static final String[] COLUMNS={"ma_so_bhxh","ma_so_bhxh_cu","ho_ten","ngay_sinh_chuan","so_dinh_danh","so_dien_thoai","gioi_tinh","is_alive","ma_tinh","ma_xa","ten_dan_toc","ten_quoc_tich","dq_flags","normalization_status"};
    static final String[] JSON={"maSoBhxh","maSoBhxhCu","hoTen","ngaySinh","soDinhDanh","soDienThoai","gioiTinh","isAlive","maTinh","maXa","danToc","quocTich","dqFlags","normalizationStatus"};
    static final String SELECT=String.join(",",COLUMNS);
    private final JdbcTemplate jdbc;
    public C360Repository(JdbcTemplate jdbc){this.jdbc=jdbc;}
    public List<Map<String,Object>> lookup(String column,String code) {
        // Tên cột chỉ từ allowlist nội bộ; dữ liệu client luôn là bind parameter.
        if(!Set.of("ma_so_bhxh","ma_so_bhxh_cu","so_dinh_danh").contains(column))throw new IllegalArgumentException();
        return jdbc.queryForList("SELECT "+SELECT+" FROM "+TABLE+" WHERE "+column+"=? ORDER BY ma_so_bhxh LIMIT 101",code);
    }
    public List<Map<String,Object>> search(String name,List<String> prefixes,String pattern,int limit) {
        var marks=String.join(",",Collections.nCopies(prefixes.size(),"?"));
        var params=new ArrayList<Object>();params.add(name);params.addAll(prefixes);params.add(pattern);params.add(limit);
        return jdbc.queryForList("SELECT "+SELECT+" FROM "+TABLE+" WHERE ho_ten_unsigned=? AND (ngay_sinh_chuan IN ("+marks+") OR ngay_sinh_chuan LIKE ?) ORDER BY ma_so_bhxh LIMIT ?",params.toArray());
    }
    public List<Map<String,Object>> changes(OffsetDateTime low,long id,OffsetDateTime until,int limit) {
        return jdbc.queryForList("SELECT "+SELECT+",id,sync_date FROM "+TABLE+" WHERE (sync_date,id)>(?,?) AND sync_date<=? ORDER BY sync_date,id LIMIT ?",low,id,until,limit);
    }
    @Transactional(readOnly=true,timeout=5)
    public List<Map<String,Object>> key(String hash) {
        return jdbc.queryForList("SELECT key_hash,client_id,scopes FROM c360_api.api_keys WHERE key_hash=? AND enabled",hash);
    }
    public static Map<String,Object> publicRow(Map<String,Object> row) {
        var result=new LinkedHashMap<String,Object>();
        for(int i=0;i<COLUMNS.length;i++)result.put(JSON[i],row.get(COLUMNS[i]));
        return result;
    }
}
