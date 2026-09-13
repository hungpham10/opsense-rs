-- Short-lived sessions cho REPL access_token — 5 phút TTL.
-- MySQL: PARTITION BY RANGE theo UNIX_TIMESTAMP(created_at).
-- Mỗi partition = 1 giờ. EVENT Scheduler gọi drop_expired_partitions hàng ngày.

CREATE TABLE IF NOT EXISTS sys_short_sessions (
    id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    user_id     VARCHAR(255) NOT NULL,
    session_id  VARCHAR(64) NOT NULL,
    token_hash  VARCHAR(64) NOT NULL,
    expires_at  TIMESTAMP NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_token_hash (token_hash, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
    -- Partitions sẽ được thêm bằng procedure. 0 = fallback/default.
    PARTITION p_default VALUES LESS THAN (0)
);

-- Procedure: tạo partition cho N giờ tới
DELIMITER $$
CREATE PROCEDURE ensure_short_session_partitions(IN hours_ahead INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE part_ts TIMESTAMP;
    DECLARE part_name VARCHAR(32);
    DECLARE unix_end BIGINT;

    WHILE i <= hours_ahead DO
        SET part_ts := DATE_FORMAT(DATE_ADD(NOW(), INTERVAL i HOUR), '%Y-%m-%d %H:00:00');
        SET part_name := CONCAT('p', DATE_FORMAT(part_ts, '%Y%m%d%H'));
        SET unix_end := UNIX_TIMESTAMP(part_ts) + 3600;

        -- DROP + CREATE để recreate partition mới nhất mỗi lần
        SET @sql := CONCAT(
            'ALTER TABLE sys_short_sessions REORGANIZE PARTITION p_default INTO (',
            'PARTITION ', part_name, ' VALUES LESS THAN (', unix_end, '),',
            'PARTITION p_default VALUES LESS THAN (0))'
        );
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i := i + 1;
    END WHILE;
END$$
DELIMITER ;

-- Procedure: drop partitions cũ hơn 7 ngày
DELIMITER $$
CREATE PROCEDURE drop_expired_short_session_partitions()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE part_name VARCHAR(64);
    DECLARE part_desc VARCHAR(64);
    DECLARE cutoff_ts TIMESTAMP;
    DECLARE cur CURSOR FOR
        SELECT PARTITION_NAME, PARTITION_DESCRIPTION
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'sys_short_sessions'
          AND PARTITION_NAME != 'p_default';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done := TRUE;

    SET cutoff_ts := DATE_SUB(NOW(), INTERVAL 7 DAY);

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO part_name, part_desc;
        IF done THEN LEAVE read_loop; END IF;
        IF part_desc != 'MAXVALUE'
           AND FROM_UNIXTIME(CAST(part_desc AS UNSIGNED)) < cutoff_ts
        THEN
            SET @sql := CONCAT(
                'ALTER TABLE sys_short_sessions DROP PARTITION ', part_name
            );
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
    END LOOP;
    CLOSE cur;
END$$
DELIMITER ;

-- Tự động tạo partitions cho 24 giờ tới (gọi 1 lần khi schema apply)
CALL ensure_short_session_partitions(24);

-- EVENT: chạy mỗi ngày lúc 03:00 để cleanup cũ + thêm partitions mới
SET GLOBAL event_scheduler = ON;
CREATE EVENT IF NOT EXISTS ev_cleanup_short_sessions
ON SCHEDULE EVERY 1 DAY STARTS '2026-09-04 03:00:00'
DO
BEGIN
    CALL drop_expired_short_session_partitions();
    CALL ensure_short_session_partitions(24);
END;
