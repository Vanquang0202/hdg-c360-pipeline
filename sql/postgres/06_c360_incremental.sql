-- Bật enqueue riêng cho nhân khẩu; cùng transaction với thay đổi Mirror.
ALTER TABLE c360.c360_sync_control ADD COLUMN catalog_digest text;
ALTER TABLE c360.c360_sync_control ADD COLUMN last_incremental_run uuid;
CREATE FUNCTION c360.enqueue_mirror_change() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog, c360 AS $enqueue$
BEGIN
 IF TG_TABLE_SCHEMA <> 'public' OR TG_TABLE_NAME <> 'mirror_hdg_nhankhau' THEN
  RAISE EXCEPTION 'Unexpected enqueue relation';
 END IF;
 IF TG_OP = 'DELETE' THEN
  INSERT INTO c360.c360_change_queue(ma_cd) VALUES(OLD.ma_cd);
  RETURN OLD;
 END IF;
 INSERT INTO c360.c360_change_queue(ma_cd) VALUES(NEW.ma_cd);
 IF TG_OP='UPDATE' AND OLD.ma_cd IS DISTINCT FROM NEW.ma_cd THEN
  INSERT INTO c360.c360_change_queue(ma_cd) VALUES(OLD.ma_cd);
 END IF;
 RETURN NEW;
END;
$enqueue$;
CREATE TRIGGER c360_enqueue_nhankhau AFTER INSERT OR UPDATE OR DELETE
ON public.mirror_hdg_nhankhau FOR EACH ROW EXECUTE FUNCTION c360.enqueue_mirror_change();
