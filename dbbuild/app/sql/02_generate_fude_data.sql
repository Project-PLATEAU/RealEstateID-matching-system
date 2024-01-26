-- 02_generate_fude_data.sql
-- 14条地図データを格納した fude_original テーブルから
-- 筆ポリゴンマスタ fude_master テーブルを生成する。

set client_min_messages = warning;

DROP TABLE IF EXISTS fude_geometry;
CREATE TEMPORARY TABLE fude_geometry AS
SELECT
    concat(市町村c, 大字コード, 丁目コード, 小字コード, 予備コード, 地番) AS cd,
    MIN(ogc_fid) AS fid,
    ST_Multi(ST_CollectionExtract(ST_Union(ST_MakeValid(wkb_geometry)))) AS geom
FROM
    fude_original
WHERE 地番 SIMILAR TO '[0-9\-]+'
GROUP BY 1;
CREATE INDEX idx_fude_geometry_geom ON fude_geometry USING gist(geom);

INSERT INTO fude_master
SELECT
    g.cd,
    g.fid AS fid,
    f.市町村c AS citycode,
    f.市町村名 AS city,
    f.大字名 AS oaza,
    f.丁目名 AS chome,
    f.小字名 AS aza,
    f.地番 AS chiban,
    NULL,
    ST_Centroid(g.geom) AS center,
    g.geom AS region
FROM fude_geometry g
LEFT JOIN fude_original f
ON g.fid=f.ogc_fid;

CREATE INDEX idx_fude_master_citycode ON fude_master (citycode COLLATE "C");
CREATE INDEX idx_fude_master_center ON fude_master USING gist (center);
CREATE INDEX idx_fude_master_region ON fude_master USING gist (region);

--　対象自治体コードを列挙する

DROP TABLE IF EXISTS citycodes;
CREATE TABLE citycodes AS
SELECT citycode
FROM fude_master
GROUP BY citycode
ORDER BY citycode ASC;
