-- 01_init_tables.sql
-- 必要なテーブル一式の初期化を行う。

set client_min_messages = warning;

-- 登記所備付地図原データテーブル
DROP TABLE IF EXISTS fude_original CASCADE;
-- このテーブルは ogr2ogr で作成するため
-- CREATE TABLE は実行しない。

-- 土地登記原データテーブル
DROP TABLE IF EXISTS tochi_original CASCADE;
CREATE TABLE tochi_original(
    元の所在名 VARCHAR NOT NULL,
    所在 VARCHAR NOT NULL,
    地番 VARCHAR NOT NULL,
    地目 VARCHAR,
    変更履歴 VARCHAR(2048),
    登録の日 DATE,
    不動産番号 VARCHAR(13) NOT NULL,
    地積 REAL,
    削除フラグ BOOLEAN,
    変更後 VARCHAR,
    管轄登記所コード CHAR(4),
    表示履歴番号 SMALLINT,
    表示履歴地番 VARCHAR,
    住所コード CHAR(17),
    データ抽出日 DATE,
    市区町村コード CHAR(5),
    団体コード CHAR(6),
    都道府県名 VARCHAR(12),
    市区町村 VARCHAR(256),
    所在外字コード BOOLEAN,
    町字id CHAR(7),
    町字 VARCHAR(256),
    地目コード CHAR(2)
);

-- 土地番号テーブル
DROP TABLE IF EXISTS tochi_bango CASCADE;
CREATE TABLE tochi_bango (
    市区町村コード CHAR(5) NOT NULL,
    所在 VARCHAR NOT NULL,
    地番 VARCHAR NOT NULL,
    登録の日 DATE,
    土地id VARCHAR(18) NOT NULL,
    筆コード VARCHAR(32) DEFAULT NULL
);

-- 建物登記原データテーブル
DROP TABLE IF EXISTS tatemono_original CASCADE;
CREATE TABLE tatemono_original(
    不動産番号 VARCHAR(13) NOT NULL PRIMARY KEY,
    分類 VARCHAR(1024) NOT NULL,
    origin_所在 VARCHAR(1024),
    所在 VARCHAR(1024),
    所在及び地番 VARCHAR(2048),
    都道府県名 VARCHAR(12),
    団体コード CHAR(6),
    市区町村コード CHAR(6),
    市区町村 VARCHAR(1024) NOT NULL,
    町字id CHAR(7),
    町字 VARCHAR(1024),
    家屋番号 VARCHAR(1024),
    家屋番号_num VARCHAR(1024),
    種類 VARCHAR(1024),
    構造 VARCHAR(1024),
    構成材料 VARCHAR(1024),
    屋根の種類 VARCHAR(1024),
    階数 VARCHAR(1024),
    床面積 VARCHAR(1024),
    管轄登記所コード CHAR(4),
    データ抽出日 VARCHAR(1024),
    登記の日付_所在 VARCHAR(1024),
    登記の日付_家屋番号 VARCHAR(1024),
    登記の日付_主である建物の表示 VARCHAR(1024),
    原因及びその日付_主である建物の表示 VARCHAR(2048),
    原因及びその日付_所在 VARCHAR(1024),
    備考_主である建物の表示 VARCHAR(1024),
    最終更新日 VARCHAR(1024),
    専有フラグ BOOLEAN,
    付属建物有無 BOOLEAN,
    ファイル名 VARCHAR(1024)
);

-- 建物ID対応データテーブル
DROP TABLE IF EXISTS propertyid_master CASCADE;
CREATE TABLE propertyid_master (
    fudosan_id VARCHAR(18) PRIMARY KEY,
    fudosan_bango VARCHAR(13) NOT NULL,
    bldg_id VARCHAR(14) NOT NULL,
    bldg_number VARCHAR(8),
    room_number VARCHAR(8)
);

-- 建物データテーブル
DROP TABLE IF EXISTS building_master CASCADE;
CREATE TABLE building_master (
    bldg_id VARCHAR(18) PRIMARY KEY,
    tatemono_id VARCHAR(18),
    n_touki SMALLINT,
    n_kyotaku SMALLINT,
    bunrui VARCHAR(8),
    shozai_oyobi_chiban VARCHAR(1024) NOT NULL,
    shikuchoson_code CHAR(5),
    machiaza_id CHAR(7),
    kaoku_bango VARCHAR(40),
    shurui VARCHAR(1024),
    kousei_zairyo VARCHAR(1024),
    yane_no_shurui VARCHAR(1024),
    yuka_menseki TEXT,
    kaisuu VARCHAR(1024),
    gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji VARCHAR(80),
    fuzoku_tatemono_umu BOOLEAN,
    address_level SMALLINT,
    fude_status SMALLINT,
    floors SMALLINT,
    floors_below_ground SMALLINT,
    floor_space REAL,
    total_floor_space REAL,
    usage_code SMALLINT,
    structure_code SMALLINT,
    construction_year SMALLINT,
    region GEOMETRY(MultiPolygon,4326),
    center GEOMETRY(Point,4326)
);

-- 筆マスターテーブル
DROP TABLE IF EXISTS fude_master CASCADE;
CREATE TABLE fude_master (
    cd varchar(32) PRIMARY KEY,
    fid integer NOT NULL,
    citycode varchar(5),
    city varchar(10),
    oaza varchar(16),
    chome varchar(16),
    aza varchar(16),
    chiban varchar(16),
    tochi_id VARCHAR(18),
    center geometry(Point,4326),
    region geometry(MultiPolygon,4326)
);

-- 建物所在地データテーブル
DROP TABLE IF EXISTS tatemono_shozaichi CASCADE;
CREATE TABLE tatemono_shozaichi (
    bldg_id VARCHAR(14) NOT NULL,    -- 建物ID
    chiban_seq SMALLINT,             -- 地番連番
    chiban VARCHAR NOT NULL,         -- 建物の地番
    address_seq SMALLINT,            -- 住所連番
    address VARCHAR(80) NOT NULL,    -- 地番にマッチした住所
    city_code VARCHAR(5),            -- 市区町村コード
    fude_code VARCHAR(32),           -- 筆コード（マッチした場合）
    lon double precision NOT NULL,   -- 住所の代表点経度
    lat double precision NOT NULL,   -- 住所の代表点緯度
    level SMALLINT NOT NULL,         -- 住所のレベル
    fude_status SMALLINT             -- 筆コード付与ステータスコード
);

-- PLATEAU 属性用データテーブル
-- 建物データから生成した PLATEAU 属性と対応できるデータを DB に登録する。
DROP TABLE IF EXISTS plateau_attributes CASCADE;
CREATE TABLE plateau_attributes(
    bldg_id VARCHAR(14) NOT NULL PRIMARY KEY,
    floors SMALLINT,
    floors_below_ground SMALLINT,
    floor_space REAL,
    total_floor_space REAL,
    structure_code SMALLINT,
    usage_code SMALLINT,
    construction_year SMALLINT
);
