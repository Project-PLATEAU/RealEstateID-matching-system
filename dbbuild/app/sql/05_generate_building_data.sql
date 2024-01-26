-- 05_generate_building_data.sql
-- 建物データを生成する。

set client_min_messages = warning;

-- 筆ポリゴンが存在する自治体以外削除する
DELETE FROM tatemono_original
WHERE 市区町村コード NOT IN (
    SELECT citycode
    FROM citycodes
);

-- 建物不動産登記に対して
-- 「所在及び地番」の先頭地番部分を抽出し、アスキー文字に統一して
-- sentou_chiban に格納する。
-- 「家屋番号」の地番部分（所在部分を除外した部分）を抽出し、
-- アスキー文字に統一して kaoku_chiban に格納する。
-- 一部の家屋番号表記で '－' が使われているので 'の' に置換する。
DROP TABLE IF EXISTS tmp_trans;
CREATE TEMPORARY TABLE tmp_trans AS SELECT
    不動産番号 AS fudosan_bango,
    所在及び地番 AS shozai_oyobi_chiban,
    TRANSLATE(
        REPLACE(所在及び地番, 都道府県名, ''),
        '０１２３４５６７８９〇一二三四五六七八九壱弐参ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
        '01234567890123456789123ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    )::varchar AS clensed_shozai_oyobi_chiban,
    REPLACE(REPLACE(
        SUBSTRING(
            TRANSLATE(
                所在及び地番,
                '０１２３４５６７８９〇一二三四五六七八九壱弐参ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
                '01234567890123456789123ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
            ) FROM '　[0-9]+番地[イロハ0-9]*'
         ), '　', ''
    ), '番地', '番')::varchar AS sentou_chiban,
    家屋番号 AS kaoku_bango,
    REPLACE(
        SUBSTRING(
            TRANSLATE(
                REPLACE(家屋番号, '番地', '番'),
                '０１２３４５６７８９〇一二三四五六七八九壱弐参ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
                '01234567890123456789123ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
            ) FROM '[0-9]+番.*'
        ), '－', 'の'
    )::varchar AS kaoku_chiban,
    分類 AS bunrui,
    分類='区建' 
    -- AND (
    --     床面積 LIKE '%階部分%' OR 階数='１階建' OR 所在及び地番 LIKE '%号棟%'
    -- )
    AS is_part_of_bldg
FROM tatemono_original;

-- 家屋番号の表記で、枝番の前に「の」が入っていると支号と区別がつかないので
-- 先頭地番と比較して「（地番）番（枝番）...」に整形する。
UPDATE tmp_trans
SET kaoku_chiban=REPLACE(kaoku_chiban, '番の', '番')
WHERE
    sentou_chiban LIKE '%番_%' AND kaoku_chiban LIKE '%番の%' AND
    STRPOS(
        REPLACE(kaoku_chiban, '番の', '番'),
        sentou_chiban
    ) = 1;

-- 逆に、枝番が不要なのに「の」が入っていないのも支号と区別がつかないので
-- 先頭地番と比較して「（地番）番の...」に整形する。
-- ただし区建ではない場合かつ支号がある場合は枝番のままとする。
UPDATE tmp_trans
SET kaoku_chiban=REPLACE(kaoku_chiban, '番', '番の')
WHERE
    sentou_chiban LIKE '%番'
    AND kaoku_chiban SIMILAR TO '[0-9]+番[^の・]+%'
    AND NOT (is_part_of_bldg=false AND kaoku_chiban SIMILAR TO '%番[^の]+の%')
    AND STRPOS(kaoku_chiban, sentou_chiban) = 1;

-- 棟を表すアルファベットのすぐ後に部屋番号が続くパターンを検出し、
-- アルファベットと数字の間に「の」を挟む
DROP TABLE IF EXISTS tmp_alpha_number;
CREATE TEMPORARY TABLE tmp_alpha_number AS
SELECT
    sq.clensed_shozai_oyobi_chiban,
    sq.total,
    sq.mat
FROM (SELECT
        clensed_shozai_oyobi_chiban,
        COUNT(kaoku_chiban) AS total,
        COUNT(CASE WHEN kaoku_chiban SIMILAR TO '%の[A-Z][0-9]%' THEN true ELSE null END) AS mat
    FROM tmp_trans
    GROUP BY clensed_shozai_oyobi_chiban
) sq
WHERE sq.mat > 10 AND sq.mat * 100 / sq.total > 50;

-- パターンに一致する場合は置換する
UPDATE tmp_trans
SET kaoku_chiban=CONCAT(
    LEFT(kaoku_chiban, LENGTH(SUBSTRING(kaoku_chiban FROM '.*の[A-Z][0-9]')) -1),
    'の',
    SUBSTRING(kaoku_chiban from LENGTH(SUBSTRING(kaoku_chiban FROM '.*の[A-Z][0-9]'))))
FROM tmp_alpha_number a
WHERE
    tmp_trans.clensed_shozai_oyobi_chiban=a.clensed_shozai_oyobi_chiban
    AND kaoku_chiban SIMILAR TO '%の[A-Z][0-9]%';


-- 「の」で分割し、1番目、2番目、3番目を格納する
DROP TABLE IF EXISTS tmp_split;
CREATE TEMPORARY TABLE tmp_split AS SELECT
    fudosan_bango,
    clensed_shozai_oyobi_chiban,
    kaoku_bango,
    is_part_of_bldg,
    split_part(kaoku_chiban, 'の', 1) AS chiban1,
    split_part(kaoku_chiban, 'の', 2) AS chiban2,
    split_part(kaoku_chiban, 'の', 3) AS chiban3
FROM tmp_trans;

-- 所在及び地番ごとに、 chiban2 と chiban3 の出現パターン数を数える。
-- chiban3 が '' ではない場合
DROP TABLE IF EXISTS tmp_count3;
CREATE TEMPORARY TABLE tmp_count3 AS SELECT
    clensed_shozai_oyobi_chiban,
    COUNT(DISTINCT chiban2) AS cnt_chiban2,
    COUNT(DISTINCT chiban3) AS cnt_chiban3,
    MAX(LENGTH(SUBSTRING(chiban2 FROM '[0-9]+'))) AS max_chiban2_len,
    MAX(LENGTH(SUBSTRING(chiban3 FROM '[0-9]+'))) AS max_chiban3_len,
    false AS bldg_first
FROM tmp_split
WHERE chiban3 != '' AND is_part_of_bldg=true
GROUP BY clensed_shozai_oyobi_chiban;

-- chiban2 を建物番号、chiban3 を部屋番号に利用するレコードを
-- 所在及び地番ごとに定義
UPDATE tmp_count3
SET bldg_first=true
WHERE
    cnt_chiban2 <= cnt_chiban3
    AND (max_chiban2_len IS NULL OR max_chiban2_len <= max_chiban3_len);

-- chiban3 が '' の場合
-- 数字が 3 桁以上なら部屋番号、それ以外は建物番号
DROP TABLE IF EXISTS tmp_count2;
CREATE TEMPORARY TABLE tmp_count2 AS SELECT
    clensed_shozai_oyobi_chiban,
    COUNT(DISTINCT chiban2) AS cnt_chiban2,
    ARRAY_TO_STRING((ARRAY_AGG(chiban2))[1:10], ',') AS chiban2_list,
    LENGTH(MAX(chiban2)) < 3 AS bldg_number
FROM tmp_split
WHERE chiban3 = '' AND is_part_of_bldg=true
GROUP BY clensed_shozai_oyobi_chiban;

-- chiban2 を部屋番号に利用するレコードを所在及び地番ごとに定義
UPDATE tmp_count2
SET bldg_number=false
WHERE
    clensed_shozai_oyobi_chiban SIMILAR TO '%(棟|館|[０-９]号)'
    OR cnt_chiban2 > 9;

-- 建物専有部の場合、分割した文字列のうち、
-- 出現パターン数が少ない方を建物拡張番号 (bldg_number)、
-- 出現パターン数が多い方を部屋番号 (room_number) とする。
DROP TABLE IF EXISTS tatemono_normalized;
CREATE TABLE tatemono_normalized (
    fudosan_bango VARCHAR(13),
    clensed_shozai_oyobi_chiban VARCHAR,
    kaoku_bango VARCHAR,
    chiban VARCHAR,
    bldg_number VARCHAR,
    room_number VARCHAR,
    is_part_of_bldg BOOLEAN
);

INSERT INTO tatemono_normalized
SELECT
    sp.fudosan_bango,
    sp.clensed_shozai_oyobi_chiban,
    sp.kaoku_bango,
    sp.chiban1 AS chiban,
    CASE
        WHEN sp.chiban3 = '' THEN
            CASE WHEN t2.bldg_number THEN sp.chiban2
                ELSE ''
            END
        WHEN t3.bldg_first THEN sp.chiban2
        ELSE sp.chiban3
    END AS bldg_number,
    CASE
        WHEN sp.chiban3 = '' THEN
            CASE WHEN t2.bldg_number THEN ''
                ELSE sp.chiban2
            END
        WHEN NOT t3.bldg_first THEN sp.chiban2
        ELSE sp.chiban3
    END AS room_number,
    true AS is_part_of_bldg
FROM tmp_split sp
LEFT JOIN tmp_count3 t3 ON sp.clensed_shozai_oyobi_chiban=t3.clensed_shozai_oyobi_chiban
LEFT JOIN tmp_count2 t2 ON sp.clensed_shozai_oyobi_chiban=t2.clensed_shozai_oyobi_chiban
WHERE sp.is_part_of_bldg=true;

-- 非専有部の場合は、そのまま登録する。
INSERT INTO tatemono_normalized
SELECT
    sp.fudosan_bango,
    sp.clensed_shozai_oyobi_chiban,
    sp.kaoku_bango,
    sp.chiban1 AS chiban,
    CASE WHEN sp.chiban3 != '' THEN
            sp.chiban2 || '-' || sp.chiban3
        ELSE sp.chiban2
    END AS bldg_number,
    '' AS room_number,
    false AS is_part_of_bldg
FROM tmp_split sp
WHERE sp.is_part_of_bldg=false;

-- 所在地・建物番号ごとの建物識別子（bldg_id）を生成する
DROP TABLE IF EXISTS tmp4;
CREATE TEMPORARY TABLE tmp4 AS
SELECT
    tn.fudosan_bango,
    sq.bldg_id,
    tn.bldg_number,
    tn.room_number
FROM tatemono_normalized tn
INNER JOIN (
    SELECT
        clensed_shozai_oyobi_chiban,
        bldg_number,
        CONCAT(MIN(fudosan_bango), '+') AS bldg_id
    FROM tatemono_normalized
    WHERE is_part_of_bldg=true
    GROUP BY clensed_shozai_oyobi_chiban, bldg_number
) sq
ON tn.clensed_shozai_oyobi_chiban=sq.clensed_shozai_oyobi_chiban
AND tn.bldg_number=sq.bldg_number
AND tn.is_part_of_bldg=true;

-- 建物ID対応データテーブルに登録する。
-- 全ての不動産番号に対して不動産IDと建物識別子を生成
TRUNCATE propertyid_master;
INSERT INTO propertyid_master
SELECT
    concat(t.不動産番号, '-0000') AS fudosan_id,
    t.不動産番号 AS fudosan_bango,
    COALESCE(tmp4.bldg_id, t.不動産番号) AS bldg_id,
    tmp4.bldg_number AS bldg_number,
    tmp4.room_number AS room_number
FROM tatemono_original t
LEFT JOIN tmp4
ON t.不動産番号=tmp4.fudosan_bango;

DROP TABLE tmp4;


-- 建物データを生成する。
-- 建物データは建物の不動産IDをキーとする。
-- 所在及び地番は登記のものをそのまま使う。
DROP INDEX IF EXISTS idx_building_master_citycode;
TRUNCATE building_master;
INSERT INTO building_master (
    bldg_id, n_touki, n_kyotaku, bunrui, shozai_oyobi_chiban,
    shikuchoson_code, machiaza_id, kaoku_bango, shurui,
    kousei_zairyo, yane_no_shurui, yuka_menseki, kaisuu,
    gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji,
    fuzoku_tatemono_umu
)
SELECT
    p.bldg_id,
    COUNT(p.*),
    COUNT(CASE WHEN POSITION('居宅' IN m.種類) > 0 THEN 1 ELSE 0 END),
    m.分類,
    MIN(
    	REPLACE(m.所在及び地番, m.origin_所在, m.所在)
    	ORDER BY LENGTH(m.所在及び地番) DESC
    ) AS shozai_oyobi_chiban,
    MIN(m.市区町村コード),
    MIN(m.町字id),
    MIN(m.家屋番号) AS kaoku_bango,
    STRING_AGG(DISTINCT m.種類, '　') AS shurui,
    MIN(m.構成材料 ORDER BY LENGTH(m.構成材料) DESC),
    STRING_AGG(DISTINCT m.屋根の種類, '　') AS yane_no_shurui,
    STRING_AGG(
        m.床面積, '　' ORDER BY m.不動産番号
    ) AS yuka_menseki,
    STRING_AGG(DISTINCT m.階数, '　') AS kaisuu,
    LEFT(MIN(
   		CASE
   			WHEN POSITION('新築' IN m.原因及びその日付_主である建物の表示) > 0
   			THEN m.原因及びその日付_主である建物の表示
   		END
        ), 80
    ) AS gennin_oyobi_sonohiduke,
    BOOL_OR(m.付属建物有無) AS fuzoku_tatemono
FROM propertyid_master p
LEFT JOIN tatemono_original m
ON p.fudosan_bango=m.不動産番号
WHERE m.所在及び地番 IS NOT NULL
GROUP BY 1,4;

-- 市区町村コードによる検索を高速化するためのインデックスを張る
CREATE INDEX idx_building_master_citycode
ON building_master (shikuchoson_code COLLATE "C");
