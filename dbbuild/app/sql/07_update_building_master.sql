-- 07_update_building_master.sql
-- 建物データの構造、住所レベルと空間属性（領域と代表点）を更新する
-- 更新より作り直す方が高速なので、一度別のテーブルを作ってから INSERT する。

set client_min_messages = warning;

DROP TABLE IF EXISTS agg_shozaichi;
CREATE TABLE agg_shozaichi AS
SELECT
    s.bldg_id,
    MIN(s.level) AS address_level,
    MAX(s.fude_status) AS fude_status,
    ST_MULTI(ST_UNION(f.region)) AS region,
    ST_POINT(AVG(s.lon), AVG(s.lat), 4326) AS center
FROM tatemono_shozaichi s
LEFT JOIN fude_master f
ON s.fude_code=f.cd
GROUP BY s.bldg_id;

DROP TABLE IF EXISTS building_master_new;
CREATE TABLE building_master_new AS
SELECT
    bm.bldg_id,
    CASE WHEN bm.bunrui <> '区建' THEN concat(bm.bldg_id, '-0000')
        ELSE replace(fm.tochi_id, '-0000', '-000B')
    END AS tatemono_id,
    bm.n_touki,
    bm.n_kyotaku,
    bm.bunrui,
    bm.shozai_oyobi_chiban,
    bm.shikuchoson_code,
    bm.machiaza_id,
    bm.kaoku_bango,
    bm.shurui,
    bm.kousei_zairyo,
    bm.yane_no_shurui,
    bm.yuka_menseki,
    bm.kaisuu,
    bm.gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji,
    bm.fuzoku_tatemono_umu,
    agg.address_level,
    agg.fude_status,
    pa.floors,
    pa.floors_below_ground,
    pa.floor_space,
    pa.total_floor_space,
    pa.usage_code,
    pa.structure_code,
    pa.construction_year,
    agg.region,
    agg.center
FROM building_master bm
LEFT JOIN agg_shozaichi agg
ON bm.bldg_id = agg.bldg_id
LEFT JOIN plateau_attributes pa
ON bm.bldg_id = pa.bldg_id
LEFT JOIN tatemono_shozaichi ts
ON bm.bldg_id=ts.bldg_id
LEFT JOIN fude_master fm
ON ts.fude_code=fm.cd
WHERE ts.chiban_seq=0 AND ts.address_seq=0;

DROP TABLE agg_shozaichi;
DROP TABLE plateau_attributes;

-- 建物データに登録
DROP INDEX IF EXISTS idx_building_master_region;
DROP INDEX IF EXISTS idx_building_master_center;
TRUNCATE building_master;
INSERT INTO building_master SELECT * FROM building_master_new;
CREATE INDEX idx_building_master_region ON building_master USING gist(region);
CREATE INDEX idx_building_master_center ON building_master USING gist(center);
DROP TABLE building_master_new;
