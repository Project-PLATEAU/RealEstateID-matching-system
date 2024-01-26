-- 09_create_answer.sql
-- PLATEAU 建物に建物記号を結合し、「正解条件」スコアを付与する SQL
-- plateau_answer テーブルを作成する。

DROP TABLE IF EXISTS plateau_tatemono_join;
CREATE TEMPORARY TABLE plateau_tatemono_join AS
SELECT
  p.gml_id,
  b.bldg_id,
  b.tatemono_id,
  b.shozai_oyobi_chiban,
  b.kaoku_bango,
  b.shurui,
  b.kousei_zairyo,
  b.yane_no_shurui,
  b.yuka_menseki,
  b.kaisuu,
  b.gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji,
  b.fuzoku_tatemono_umu,
  b.floors,
  b.floors_below_ground,
  b.floor_space,
  b.total_floor_space,
  b.usage_code,
  b.structure_code,
  b.construction_year,
  b.tochi_id,
  b.tochi_id_count,
  b.kobetsu_id,
  b.kobetsu_id_count,
  b.bldg_number,
  b.fude_status,
  ST_Area(p.geom::geography) AS area_geom,
  CASE
    WHEN p.building_footprint_area IS NULL
    THEN ST_Area(p.geom::geography) * 0.8
    ELSE p.building_footprint_area
  END AS footprint,
  ST_Area(ST_Intersection(p.geom, b.geom)::geography) AS area_intersection,
  -- CASE
  --   WHEN b.floors=0 THEN 1.0
  --   WHEN p.measured_height < 3.5 * b.floors THEN 3.5 - p.measured_height / b.floors
  --   WHEN p.measured_height > 3.5 * b.floors + 2.0 THEN (p.measured_height - 2.0) / b.floors - 3.5
  --   ELSE 0.0
  -- END AS height_penalty
  -- ABS(b.floors * 2.95 + 1.95 - p.measured_height) / (b.floors * 2.95 + 1.95) AS height_penalty
  ABS(b.floors * 2.95 + 1.95 - p.measured_height) AS height_penalty
FROM plateau2d p
LEFT JOIN full_id_master b ON p.geom && b.geom
-- WHERE
--   b.shikuchoson_code IN ('13201', '37201', '34208', '44204')
--   AND p.citycode IN ('13201', '37201', '34208', '44204')
;

DROP TABLE IF EXISTS plateau_answer;
CREATE TABLE plateau_answer AS
SELECT
  subq.gml_id,
  subq.bldg_id AS 建物識別記号,
  subq.shozai_oyobi_chiban AS 登記属性_所在及び地番,
  subq.kaoku_bango AS 登記属性_家屋番号,
  subq.shurui AS 登記属性_建物種類,
  subq.kousei_zairyo AS 登記属性_構成材料,
  subq.yane_no_shurui AS 登記属性_屋根の種類,
  subq.yuka_menseki AS 登記属性_床面積,
  subq.kaisuu AS 登記属性_階数,
  subq.gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji AS 登記属性_原因及びその日付,
  subq.fuzoku_tatemono_umu AS 登記属性_付属建物有無,
  subq.tatemono_id AS マッチング_建物不動産ID,
  subq.tochi_id AS マッチング_土地不動産ID,
  subq.tochi_id_count AS マッチング_土地不動産ID数,
  subq.kobetsu_id AS マッチング_個別不動産ID,
  subq.kobetsu_id_count AS マッチング_個別不動産ID数,
  subq.bldg_number AS マッチング_建物記号,
  subq.fude_status AS マッチング_筆一致状態,
  subq.floors AS 登記から計算_地上階数,
  subq.floors_below_ground AS 登記から計算_地下階数,
  subq.floor_space AS 登記から計算_床面積,
  subq.total_floor_space AS 登記から計算_延床面積,
  subq.usage_code AS 登記から計算_用途コード,
  subq.structure_code AS 登記から計算_構造種別コード,
  subq.construction_year AS 登記から計算_建築年,
  ROUND(subq.area_geom::numeric, 2) AS GMLから計算_ポリゴン面積,
  ROUND(subq.area_intersection::numeric, 2) AS マッチング_筆と交差する面積_算出,
  p.storeys_above_ground AS GML属性_地上階数,
  p.storeys_below_ground AS GML属性_地下階数,
  p.total_floor_area AS GML属性_延床面積,
  p.building_footprint_area AS GML属性_フットプリント面積,
  p.usage AS GML属性_主な使い道,
  p.building_structure_type AS GML属性_構造種別,
  p.measured_height AS GML属性_計測高さ,
  p.year_of_construction AS GML属性_建築年,
  p.citycode_file AS GMLファイル名_市区町村コード,
  p.year_file AS GMLファイル名_収録年,
  ROUND(subq.area_intersection * 100 / subq.area_geom) AS score_overwrapped,
  CASE
    WHEN subq.floors IS NOT NULL
      AND p.storeys_above_ground IS NOT NULL
      AND subq.floors = p.storeys_above_ground THEN 100
    WHEN subq.height_penalty >= 100.0 THEN 100.0
    ELSE 100 - ROUND(subq.height_penalty)
  END AS score_height,
  CASE
    WHEN p.building_footprint_area IS NOT NULL
      AND ROUND(subq.floor_space) = ROUND(p.building_footprint_area) THEN 100
    WHEN subq.floor_space=0.0 OR ABS(subq.footprint - subq.floor_space) > subq.floor_space THEN 0
    ELSE ROUND(100.0 - ABS(subq.footprint - subq.floor_space) * 100.0 / subq.floor_space)
  END AS score_area,
  CASE
    WHEN subq.construction_year IS NOT NULL 
      AND p.year_of_construction IS NOT NULL
      AND ABS(subq.construction_year - p.year_of_construction) <= 1 THEN 1
    ELSE 0
  END AS ex_score_year_of_construction,
  CASE
    WHEN subq.structure_code IS NOT NULL
      AND p.building_structure_type IS NOT NULL
      AND (subq.structure_code = p.building_structure_type OR (
        subq.structure_code IN (604, 605)
        AND p.building_structure_type IN (604, 605)
      )) THEN 1
    ELSE 0
  END AS ex_score_structure_type,
  CASE
    WHEN subq.usage_code IS NOT NULL
      AND p.usage IS NOT NULL
      AND subq.usage_code = p.usage THEN 1
    ELSE 0
  END AS ex_score_usage,
  p.geom
FROM plateau2d p
LEFT JOIN plateau_tatemono_join subq
ON subq.gml_id=p.gml_id
WHERE subq.area_intersection > 0  -- >= subq.area_geom * 0.8
;

CREATE INDEX idx_plateau_answer_geom ON plateau_answer USING gist(geom);

-- 各 PLATEAU 建物に対し、対応する不動産登記が一件しかなく、
-- 不動産登記に対応する建物も一件しかないものを選択する

-- PLATEAUポリゴンに対する建物登記が1件しかないもののリスト
DROP TABLE IF EXISTS plateau_answer_one_bldgs;
CREATE TEMPORARY TABLE plateau_answer_one_bldgs AS
SELECT gml_id, COUNT(建物識別記号) AS count FROM plateau_answer
WHERE score_overwrapped >= 90.0
GROUP BY gml_id;
DELETE FROM plateau_answer_one_bldgs WHERE count > 1;
-- 建物登記に対するPLATEAUポリゴンが1つしかないもののリスト
DROP TABLE IF EXISTS plateau_answer_one_plateau;
CREATE TEMPORARY TABLE plateau_answer_one_plateau AS
SELECT COUNT(gml_id) AS count, 建物識別記号 FROM plateau_answer
WHERE score_overwrapped >= 90.0
GROUP BY 建物識別記号;
DELETE FROM plateau_answer_one_plateau WHERE count > 1;
-- 両方の条件を満たす PLATEAU 建物のリスト
DROP TABLE IF EXISTS plateau_answer_one;
CREATE TABLE plateau_answer_one AS
SELECT * FROM plateau_answer
WHERE gml_id in (SELECT gml_id FROM plateau_answer_one_bldgs)
AND 建物識別記号 in (SELECT 建物識別記号 FROM plateau_answer_one_plateau)
AND マッチング_筆一致状態=0
ORDER BY gml_id ASC;

-- CREATE INDEX idx_plateau_answer_one_geom ON plateau_answer_one USING gist(geom);

-- 各 PLATEAU 建物に対し、最もスコアが高い不動産登記のみ残す
DROP TABLE IF EXISTS plateau_answer_best;
CREATE TABLE plateau_answer_best AS
SELECT DISTINCT ON (gml_id)
  *
FROM
  plateau_answer
ORDER BY
  gml_id ASC,
  score_overwrapped + score_height + score_area + (
    ex_score_year_of_construction + ex_score_usage + ex_score_structure_type
  ) * 15 DESC
;

CREATE INDEX idx_plateau_answer_best_geom ON plateau_answer_best USING gist(geom);


-- 各建物識別記号に対し、最もスコアが高い PLATEAU 建物のみ残す
DROP TABLE IF EXISTS plateau_answer_strict;
CREATE TABLE plateau_answer_strict AS
SELECT DISTINCT ON (建物識別記号)
  *
FROM
  plateau_answer_best
ORDER BY
  建物識別記号 ASC,
  score_overwrapped + score_height + score_area + (
    ex_score_year_of_construction + ex_score_usage + ex_score_structure_type
  ) * 15 DESC
;

CREATE INDEX idx_plateau_answer_strict_geom ON plateau_answer_strict USING gist(geom);
