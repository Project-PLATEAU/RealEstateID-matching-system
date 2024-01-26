-- 08_create_master.sql
-- これ一つで建物とのマッチングおよび ID の付与が可能な
-- full_id_master テーブルを作成する。

-- 建物識別番号, 個別不動産IDのリスト, 棟記号を列挙した
-- テーブルを作成する。
DROP TABLE IF EXISTS tmp_kobetsu_id;
CREATE TEMPORARY TABLE tmp_kobetsu_id AS
SELECT
  b.bldg_Id AS bldg_id,
  COUNT(p.fudosan_id) AS kobetsu_id_count,
  STRING_AGG(p.fudosan_id, ',' ORDER BY p.fudosan_id) AS kobetsu_id,
  MIN(p.bldg_number) as bldg_number
FROM building_master b
LEFT JOIN propertyid_master p ON b.bldg_id=p.bldg_id
GROUP BY b.bldg_id;

-- 建物識別番号, 土地不動産IDのリストを列挙したテーブルを作成する。
DROP TABLE IF EXISTS tmp_tochi_id;
CREATE TEMPORARY TABLE tmp_tochi_id AS
SELECT
  b.bldg_id AS bldg_id,
  COUNT(q1.tochi_id) AS tochi_id_count,
  STRING_AGG(q1.tochi_id, ',') AS tochi_id
FROM building_master b
LEFT JOIN 
( SELECT q2.bldg_id, q2.tochi_id, MIN(q2.chiban_seq)
  FROM
  ( SELECT t.bldg_id, f.tochi_id, t.chiban_seq
    FROM tatemono_shozaichi t
    LEFT JOIN fude_master f ON t.fude_code = f.cd
    WHERE f.tochi_id IS NOT NULL and t.fude_status < 2
    ORDER BY t.bldg_id, t.chiban_seq ASC
  ) q2
  GROUP BY q2.bldg_id, q2.tochi_id
  ORDER BY 1, 3 ASC
) q1 ON b.bldg_id=q1.bldg_id
GROUP BY b.bldg_id;

-- 建物データに土地不動産ID、個別不動産IDを追加したテーブルを作成する。
DROP TABLE IF EXISTS full_id_master;
CREATE TABLE full_id_master AS
SELECT
  b.bldg_id,
  b.tatemono_id,
  b.n_touki,
  b.n_kyotaku,
  b.bunrui,
  b.shozai_oyobi_chiban,
  b.shikuchoson_code,
  b.machiaza_id,
  b.kaoku_bango,
  b.shurui,
  b.kousei_zairyo,
  b.yane_no_shurui,
  b.yuka_menseki,
  b.kaisuu,
  b.gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji,
  b.fuzoku_tatemono_umu,
  b.address_level,
  b.fude_status,
  b.floors,
  b.floors_below_ground,
  b.floor_space,
  b.total_floor_space,
  b.usage_code,
  b.structure_code,
  b.construction_year,
  tt.tochi_id AS tochi_id,
  tt.tochi_id_count AS tochi_id_count,
  CASE
    WHEN b.bunrui = '区建' THEN tk.kobetsu_id
    ELSE NULL
  END AS kobetsu_id,
  CASE
    WHEN b.bunrui = '区建' THEN tk.kobetsu_id_count
    ELSE 0
  END AS kobetsu_id_count,
  tk.bldg_number AS bldg_number,
  b.region as geom
FROM building_master b
LEFT JOIN tmp_kobetsu_id tk ON b.bldg_id=tk.bldg_id
LEFT JOIN tmp_tochi_id tt ON b.bldg_id=tt.bldg_id;

CREATE INDEX idx_full_id_master_geom ON full_id_master USING gist(geom);
