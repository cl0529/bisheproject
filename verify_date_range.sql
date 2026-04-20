SET @start_date = '2025-11-01';
SET @end_date = '2026-04-30';
SET @start_dt = '2025-11-01 00:00:00';
SET @end_dt = '2026-04-30 23:59:59';

DROP PROCEDURE IF EXISTS verify_all_dates_in_window;

DELIMITER $$
CREATE PROCEDURE verify_all_dates_in_window()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE v_table VARCHAR(128);
  DECLARE v_column VARCHAR(128);
  DECLARE v_type VARCHAR(32);
  DECLARE v_sql LONGTEXT;

  DECLARE cur CURSOR FOR
    SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE
    FROM information_schema.COLUMNS c
    WHERE c.TABLE_SCHEMA = 'project'
      AND c.DATA_TYPE IN ('date', 'datetime', 'timestamp');

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  DROP TEMPORARY TABLE IF EXISTS date_range_violations;
  CREATE TEMPORARY TABLE date_range_violations (
    table_name VARCHAR(128),
    column_name VARCHAR(128),
    violation_count BIGINT
  );

  OPEN cur;
  read_loop: LOOP
    FETCH cur INTO v_table, v_column, v_type;
    IF done = 1 THEN
      LEAVE read_loop;
    END IF;

    IF v_type = 'date' THEN
      SET v_sql = CONCAT(
        'INSERT INTO date_range_violations(table_name,column_name,violation_count) ',
        'SELECT ''', v_table, ''',''', v_column, ''', COUNT(*) FROM `', v_table, '` ',
        'WHERE `', v_column, '` IS NOT NULL ',
        'AND (`', v_column, '` < ''', @start_date, ''' OR `', v_column, '` > ''', @end_date, ''')'
      );
    ELSE
      SET v_sql = CONCAT(
        'INSERT INTO date_range_violations(table_name,column_name,violation_count) ',
        'SELECT ''', v_table, ''',''', v_column, ''', COUNT(*) FROM `', v_table, '` ',
        'WHERE `', v_column, '` IS NOT NULL ',
        'AND (`', v_column, '` < ''', @start_dt, ''' OR `', v_column, '` > ''', @end_dt, ''')'
      );
    END IF;

    SET @run_sql = v_sql;
    PREPARE stmt FROM @run_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END LOOP;
  CLOSE cur;

  SELECT * FROM date_range_violations WHERE violation_count > 0 ORDER BY violation_count DESC;
  SELECT SUM(violation_count) AS total_violations FROM date_range_violations;
END$$
DELIMITER ;

CALL verify_all_dates_in_window();
DROP PROCEDURE IF EXISTS verify_all_dates_in_window;
