-- 03_generate_tochi_data.sql
-- 土地データを生成する。

set client_min_messages = warning;

-- 市区町村コードによる検索を高速化するためのインデックスを張る
CREATE INDEX idx_tochi_original_citycode ON tochi_original (市区町村コード COLLATE "C");

-- 市区町村コードが4桁のレコードの先頭に '0' を埋める
UPDATE tochi_original
SET 市区町村コード='0' || 市区町村コード
WHERE LENGTH(市区町村コード) = 4;

-- 筆ポリゴンが存在する自治体以外削除する
DELETE FROM tochi_original
WHERE 市区町村コード NOT IN (
    SELECT citycode
    FROM citycodes
);

-- 土地登記原データから、所在と地番でグループ化し、
-- 不要な属性を除去した土地ID対応データテーブルを作成する。
DROP INDEX IF EXISTS idx_tochi_bango_citycode;
TRUNCATE tochi_bango;
INSERT INTO tochi_bango (市区町村コード, 所在, 地番, 登録の日, 土地id)
SELECT
    MIN(市区町村コード),
    所在,
    CASE WHEN 表示履歴地番 IS NULL THEN 地番
        ELSE 表示履歴地番
    END AS 地番,
    MAX(登録の日),
    CASE WHEN LENGTH(不動産番号) = 12 THEN CONCAT('0', 不動産番号, '-0000')
        ELSE CONCAT(不動産番号, '-0000')
    END AS 土地id
FROM tochi_original
GROUP BY 2, 3, 5
ORDER BY 1 ASC;

-- 市区町村コードによる検索を高速化するためのインデックスを張る
CREATE INDEX idx_tochi_bango_citycode ON tochi_bango (市区町村コード COLLATE "C");


-- -- 不動産番号が12桁のレコードの先頭に '0' を埋める
-- UPDATE tochi_bango
-- SET 土地id='0' || 土地id
-- WHERE LENGTH(土地id) = 17;
