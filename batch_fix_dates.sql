SET @start_date = '2025-11-01';
SET @end_date = '2026-04-30';
SET @start_dt = '2025-11-01 00:00:00';
SET @end_dt = '2026-04-30 23:59:59';

DROP PROCEDURE IF EXISTS fix_all_dates_to_window;

DELIMITER $$
CREATE PROCEDURE fix_all_dates_to_window()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE v_table VARCHAR(128);
  DECLARE v_column VARCHAR(128);
  DECLARE v_type VARCHAR(32);
  DECLARE v_pk VARCHAR(128);
  DECLARE v_sql LONGTEXT;

  DECLARE cur CURSOR FOR
    SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, k.COLUMN_NAME AS pk_column
    FROM information_schema.COLUMNS c
    JOIN information_schema.KEY_COLUMN_USAGE k
      ON k.TABLE_SCHEMA = c.TABLE_SCHEMA
     AND k.TABLE_NAME = c.TABLE_NAME
     AND k.CONSTRAINT_NAME = 'PRIMARY'
     AND k.ORDINAL_POSITION = 1
    WHERE c.TABLE_SCHEMA = 'project'
      AND c.DATA_TYPE IN ('date', 'datetime', 'timestamp');

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  OPEN cur;
  read_loop: LOOP
    FETCH cur INTO v_table, v_column, v_type, v_pk;
    IF done = 1 THEN
      LEAVE read_loop;
    END IF;

    IF v_type = 'date' THEN
      SET v_sql = CONCAT(
        'WITH ranked AS (',
        'SELECT `', v_pk, '` AS pk, ROW_NUMBER() OVER (ORDER BY `', v_pk, '` DESC) AS rn ',
        'FROM `', v_table, '` ',
        'WHERE `', v_column, '` IS NOT NULL ',
        'AND (`', v_column, '` < ''', @start_date, ''' OR `', v_column, '` > ''', @end_date, ''')',
        ') ',
        'UPDATE `', v_table, '` t ',
        'JOIN ranked r ON t.`', v_pk, '` = r.pk ',
        'SET t.`', v_column, '` = DATE_SUB(''', @end_date, ''', INTERVAL MOD(r.rn - 1, 181) DAY)'
      );
    ELSE
      SET v_sql = CONCAT(
        'WITH ranked AS (',
        'SELECT `', v_pk, '` AS pk, ROW_NUMBER() OVER (ORDER BY `', v_pk, '` DESC) AS rn ',
        'FROM `', v_table, '` ',
        'WHERE `', v_column, '` IS NOT NULL ',
        'AND (`', v_column, '` < ''', @start_dt, ''' OR `', v_column, '` > ''', @end_dt, ''')',
        ') ',
        'UPDATE `', v_table, '` t ',
        'JOIN ranked r ON t.`', v_pk, '` = r.pk ',
        'SET t.`', v_column, '` = DATE_SUB(''', @end_dt, ''', INTERVAL MOD(r.rn - 1, 260640) MINUTE)'
      );
    END IF;

    SET @run_sql = v_sql;
    PREPARE stmt FROM @run_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END LOOP;

  CLOSE cur;
END$$
DELIMITER ;

CALL fix_all_dates_to_window();
DROP PROCEDURE IF EXISTS fix_all_dates_to_window;
