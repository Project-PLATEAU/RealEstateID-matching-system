import boto3
import os
import subprocess
import psycopg2
import datetime
import shutil

from dotenv import load_dotenv
from lxml import etree
from pytz import timezone

input_dir = f"data/input/{os.environ.get('ESTATE_ID_USER_ID')}/{os.environ.get('ESTATE_ID_SESSION_ID')}"
output_dir = f"data/output/{os.environ.get('ESTATE_ID_USER_ID')}/{os.environ.get('ESTATE_ID_SESSION_ID')}"

namespaces = {'gml': 'http://www.opengis.net/gml',
              'bldg': 'http://www.opengis.net/citygml/building/2.0',
              'gen': 'http://www.opengis.net/citygml/generics/2.0',
              'uro': 'https://www.geospatial.jp/iur/uro/3.0',
              'real': 'http://www.example.com/citygml/realpropertyid/2.0'}

def main():
    load_dotenv()

    download_file_from_s3()
    gml2postgis()

    print("initialize")
    create_working_table()

    print("マッチング開始")
    use_other_data_flag = os.environ.get('USE_ESTATE_ID_CONFIRMATION_SYSTEM')
    if use_other_data_flag == "1":
        match_to_estate_id_confirmation_system()
    else:
        match_to_estate_id()
        calc_algorithm_flag()

    add_estate_id_to_gml()

    print("desirialize")


def download_file_from_s3():
    """指定のS3バケットからinput_dir以下のgmlファイルをダウンロードする"""
    s3_resource = boto3.resource(
        service_name='s3'
    )
    print(input_dir)

    bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])

    for obj in bucket.objects.filter(Prefix=input_dir):
        if obj.key.endswith('.gml'):
            if not os.path.exists(os.path.dirname(obj.key)):
                os.makedirs(os.path.dirname(obj.key))
            print("Downloading {}...".format(obj.key))
            bucket.download_file(obj.key, obj.key)
    print("Download completed.")


def create_gml_removed_tag(gml: str, dst: str,
                           rm_tag: str = "app:appearanceMember"
                           ) -> bool:
    """ogr2ogr コマンドの妨げになっているタグ(ex: <app:appearanceMember>)を除いたgmlを作成する。"""
    with open(gml) as f:
        data = f.read()
    rm_tag = "app:appearanceMember"
    rm_tag_idx = data.find(rm_tag)
    result = rm_tag_idx != -1
    if result:
        rm_start_idx = data.rfind("\n", 0, rm_tag_idx) + 1
        rm_end_idx = data.find("\n", data.find(f"/{rm_tag}", rm_start_idx)) + 1
        with open(dst, 'w') as f:
            f.write(data[:rm_start_idx])
            f.write(data[rm_end_idx:])
    return result



def gml2postgis():
    """S3バケットからダウンロードしたgmlファイルをPostGISにインポートする"""
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                   os.environ["DBNAME"], os.environ["USER"],
                                                                   os.environ["PASSWORD"])) as conn:

        sql_create_table = '''
        CREATE TABLE IF NOT exists public.building_citygml (
            gml_id character varying NOT NULL,
            "建物id" character varying(16),
            measuredheight double precision,
            measuredheight_uom character varying(1),
            filename character varying(255),
            usage integer NULL,
            buildingStructureType_uro integer NULL,
            buildingFootprintArea_uro double precision NULL,
            storeysAboveGround integer NULL,
            storeysBelowGround integer NULL,
            yearOfConstruction integer NULL,
            lod0geom public.geometry(Geometry, 4326),
            user_id character varying(255) NOT NULL,
            session_id character varying(255) NOT NULL
            );

            ALTER TABLE public.building_citygml OWNER TO postgres;
            CREATE INDEX IF NOT exists building_citygml_lod0geom_geom_idx ON public.building_citygml USING gist (lod0geom);
            CREATE INDEX IF NOT exists building_citygml_idx1 ON public.building_citygml (filename, user_id, session_id);
            DROP TABLE IF EXISTS public.building;
        '''
        conn.cursor().execute(sql_create_table)

    for file in os.listdir(input_dir):
        if file.endswith('.gml'):
            input_file = os.path.join(input_dir, file)
            tree = etree.parse(input_file)
            root = tree.getroot()

            # bldg:lod0RoofEdgeの場合
            if root.find('.//bldg:lod0RoofEdge', namespaces) is not None:
                print("lod0RoofEdge")
                lod0_type = "lod0RoofEdge"

            # bldg:lod0FootPrintの場合
            elif root.find('.//bldg:lod0FootPrint', namespaces) is not None:
                print("lod0FootPrint")
                lod0_type = "lod0FootPrint"

            print(f"{file}をインポート中...")
            cp = subprocess.run(
                f'ogr2ogr -forceNullable -f "PostgreSQL" PG:"host={os.environ["HOST"]} port={os.environ["PORT"]} dbname={os.environ["DBNAME"]} user={os.environ["USER"]} password={os.environ["PASSWORD"]}" "{input_file}" -oo GFS_TEMPLATE=src/{lod0_type}.gfs',
                shell=True)

            with psycopg2.connect(
                    "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                           os.environ["DBNAME"], os.environ["USER"],
                                                                           os.environ["PASSWORD"])) as conn:

                sql_move_table = f'''
                ALTER TABLE building ADD COLUMN user_id character varying(255);
                ALTER TABLE building ADD COLUMN session_id character varying(255);
                ALTER TABLE building ADD COLUMN IF NOT EXISTS usage integer;
                ALTER TABLE building ADD COLUMN IF NOT EXISTS buildingStructureType_uro double precision;
                ALTER TABLE building ADD COLUMN IF NOT EXISTS buildingFootprintArea_uro double precision;
                ALTER TABLE building ADD COLUMN IF NOT EXISTS storeysAboveGround integer;
                ALTER TABLE building ADD COLUMN IF NOT EXISTS storeysBelowGround integer;
                ALTER TABLE building ADD COLUMN IF NOT EXISTS yearOfConstruction integer;

                UPDATE building SET filename='{file}';
                UPDATE building SET user_id='{os.environ.get('ESTATE_ID_USER_ID')}';
                UPDATE building SET session_id='{os.environ.get('ESTATE_ID_SESSION_ID')}';

                INSERT INTO building_citygml (
                gml_id,
                "建物id",
                measuredheight,
                measuredheight_uom,
                filename,
                usage,
                buildingStructureType_uro,
                buildingFootprintArea_uro,
                storeysAboveGround,
                storeysBelowGround,
                yearOfConstruction,
                lod0geom,
                user_id,
                session_id)
                SELECT
                gml_id,
                "建物id",
                measuredheight,
                measuredheight_uom,
                filename,
                COALESCE(usage, 0),
                COALESCE(buildingStructureType_uro, 0),
                COALESCE(buildingfootprintarea_uro, 0),
                COALESCE(storeysAboveGround, 0),
                COALESCE(storeysBelowGround, 0),
                COALESCE(yearOfConstruction, 0),
                ST_Transform(lod0geom, 4326),
                user_id,
                session_id FROM building;
                '''
                try:
                    conn.cursor().execute(sql_move_table)
                    print(f"{file}をインポート完了")
                except psycopg2.errors.ProgrammingError as err:
                    conn.cursor().execute("ROLLBACK;DROP TABLE building;")
                    tmp_gml = 'tmp.gml'
                    if create_gml_removed_tag(input_file, tmp_gml):
                        try:
                            cmd = cp.args.replace(input_file, tmp_gml)
                            subprocess.run(cmd, shell=True)
                            try:
                                conn.cursor().execute(sql_move_table)
                            except Exception as err:
                                print(err)
                        finally:
                            if os.path.exists(tmp_gml):
                                os.remove(tmp_gml)
                    else:
                        print(err)
                        print(f"{file}をインポート失敗")
                except Exception as err:
                    sql = 'ROLLBACK;'
                    conn.cursor().execute(sql)
                    print(err)
                    print(f"{file}をインポート失敗")
                finally:
                    conn.cursor().execute("DROP TABLE building;")

def create_working_table():
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                   os.environ["DBNAME"], os.environ["USER"],
                                                                   os.environ["PASSWORD"])) as conn:
        print("マッチング用のテーブルを作成")
        # マッチング用のテーブルを作成
        create_sql = '''
        CREATE TABLE IF NOT exists building_citygml_matched (
            gml_id varchar NOT NULL,
            建物id varchar(16) NULL,
            lod0geom public.geometry(geometry, 4326) NULL,
            filename varchar(255) NULL,
            user_id varchar(255) NOT NULL,
            session_id varchar(255) NOT NULL,
            tatemono_id text NULL,
            bldg_id varchar(18) NOT NULL,
            bunrui varchar(8) NULL,
            n_touki integer NULL,
            floor_space float4 NULL,
            structure_code integer NULL,
            height double precision NULL,
            floors integer NULL,
            region public.geometry(multipolygon, 4326) NULL,
            fudosan_id text NOT NULL,
            algorithm_flag varchar(2) NULL,
            score_fude integer NULL,
            score_high integer NULL,
            score_wide integer NULL,
            score_total integer NULL,
            score_total_max integer NULL,
            matching_count integer NULL DEFAULT 0,

            citygml_floors integer NULL,
            citygml_floors_below_ground integer NULL,
            citygml_floor_space float4 NULL,
            citygml_usage_code integer NULL,
            citygml_structure_code integer NULL,
            citygml_construction_year integer NULL,

            storeysAboveGround integer NULL,
            storeysBelowGround integer NULL,
            buildingFootprintArea float4 NULL,
            usage integer NULL,
            buildingStructureType_uro integer NULL,
            yearOfConstruction integer NULL,
            fudosan_id_hash varchar(32) NULL
        );
        CREATE INDEX IF NOT exists building_citygml_matched_idx1 ON building_citygml_matched (gml_id, filename, user_id, session_id);
        CREATE INDEX IF NOT exists building_citygml_matched_idx2 ON building_citygml_matched (gml_id, user_id, session_id);
        CREATE INDEX IF NOT exists building_citygml_matched_idx3 ON building_citygml_matched (gml_id);
        CREATE INDEX IF NOT exists building_citygml_matched_idx4 ON building_citygml_matched (user_id, session_id, filename);
        CREATE INDEX IF NOT exists building_citygml_matched_idx5 ON building_citygml_matched (user_id, session_id);
        '''
        conn.cursor().execute(create_sql)

def match_to_estate_id_confirmation_system():
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                   os.environ["DBNAME"], os.environ["USER"],
                                                                   os.environ["PASSWORD"])) as conn:
        print("*** 不動産ID確認システムのデータでマッチング ***")

        # 環境変数から条件に設定する閾値を取得
        rate_limit = int(os.environ.get('ESTATE_ID_CONFIRMATION_SYSTEM_RATE_LIMIT'))
        area_min = int(os.environ.get('ESTATE_ID_CONFIRMATION_SYSTEM_AREA_MIN'))
        area_max = int(os.environ.get('ESTATE_ID_CONFIRMATION_SYSTEM_AREA_MAX'))

        print(f"ESTATE_ID_CONFIRMATION_SYSTEM_RATE_LIMIT: {rate_limit}")
        print(f"ESTATE_ID_CONFIRMATION_SYSTEM_AREA_MIN: {area_min}")
        print(f"ESTATE_ID_CONFIRMATION_SYSTEM_AREA_MAX: {area_max}")
        # ファイル・セッションID・ユーザID毎にマッチング処理を行い、データを格納する
        for file in os.listdir(input_dir):
            if file.endswith('.gml'):
                estate_id_user_id = os.environ.get('ESTATE_ID_USER_ID')
                estate_id_session_id = os.environ.get('ESTATE_ID_SESSION_ID')

                print(f"file: {file}")

                create_sql = f'''
                INSERT INTO building_citygml_matched
                SELECT
                    subq.gml_id,
                    subq.建物id,
                    subq.lod0geom AS lod0geom,
                    subq.filename,
                    subq.user_id,
                    subq.session_id,
                    subq.fudosan_id as tatemono_id,
                    '' as bldg_id,
                    subq.bunrui,
                    0 as n_touki,
                    0 as floor_space,
                    0 as structure_code,
                    COALESCE(measuredheight, 0) as height,
                    subq.measuredheight as floors,
                    NULL as region,
                    '' as fudosan_id,
                    'A' as algorithm_flag,
                    0 as score_fude,
                    0 as score_high,
                    0 as score_wide,
                    0 as score_total,
                    0 as citygml_floors,
                    0 as citygml_floors_below_ground,
                    0 as citygml_floor_space,
                    0 as citygml_usage_code,
                    0 as citygml_structure_code,
                    0 as yearOfConstruction,
                    0 as usage,
                    0 as buildingStructureType_uro,
                    0 as buildingFootprintArea,
                    0 as storeysAboveGround,
                    0 as storeysBelowGround,
                    0 as yearOfConstruction
                FROM (
                    SELECT
                    p.gml_id,
                    p.建物id,
                    p.lod0geom,
                    p.filename,
                    p.user_id,
                    p.session_id,
                    h.不動産IDリスト AS fudosan_id,
                    h.所在及び地番リスト AS shozai_oyobi_chiban,
                    ROUND(100 * ST_Area(ST_Intersection(p.lod0geom, h.geom)) / ST_Area(p.lod0geom)) AS rate,
                    h.geom,
                    b.bunrui,
                    p.measuredheight
                    FROM
                    building_citygml p
                    LEFT JOIN
                    fudosan_id_kakunin_system_build_grouped h ON p.lod0geom && h.geom
                    LEFT JOIN fudosan_id_kakunin_system_build b ON h.最小不動産番号 = b.fudosan_bango
                    WHERE
                    h.不動産ID数=1
                    AND p.filename = '{file}'
                    AND p.user_id = '{estate_id_user_id}'
                    AND p.session_id = '{estate_id_session_id}'
                ) subq
                WHERE subq.rate > 0
                AND subq.rate >= {rate_limit}
                AND ST_Area(subq.lod0geom) BETWEEN (ST_Area(subq.geom) * {area_min}/100) AND (ST_Area(subq.geom) * {area_max}/100)
                '''
                conn.cursor().execute(create_sql)

                # 取得した情報について、土地不動産IDを求めて設定する更新クエリを発行
                create_sql = f'''
                UPDATE building_citygml_matched
                SET fudosan_id = subq.tochi_id,
                region = subq.fude_geom
                FROM (
                    SELECT DISTINCT
                    fudosan_id, tochi_id, bunrui, fude_geom
                    FROM
                    fudosan_id_kakunin_system_build_check
                    WHERE rate > 0
                    AND tochi_id IS NOT NULL
                ) AS subq
                WHERE SUBSTRING(tatemono_id, 1, 18)= subq.fudosan_id
                AND filename = '{file}'
                AND user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                and algorithm_flag = 'A'
                '''
                conn.cursor().execute(create_sql)

                # マッチングデータ追加件数チェック用SQL
                count_sql = f'''
                SELECT count(*) AS row_count
                FROM building_citygml_matched
                WHERE filename = '{file}'
                AND user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                '''
                cursor = conn.cursor()
                cursor.execute(count_sql)
                row = cursor.fetchall()
                len_matched = len(row)
                if len_matched > 0:
                    print(f"マッチングデータ追加件数: {row[0][0]}件")
                else:
                    print("マッチングデータ追加件数: 0件")


def get_citygml_bbox(input_file: str) -> str:
    """gmlファイルの <gml:lowerCorner>, <gml:upperCorner> からBBOXをWKT形式で作成します。"""
    lower_corner_end = "</gml:lowerCorner>"
    lower_corner_end_length = len(lower_corner_end)
    upper_corner_end = "</"
    lower_corner_end_idx = -1
    upper_corner_end_idx = -1
    size = 4096
    cur = 0
    with open(input_file) as f:
        data = f.read(size)
        lower_corner_end_idx = data.find(lower_corner_end)
        cur += size
        while lower_corner_end_idx == -1 and cur == f.tell():
            data += f.read(size)
            lower_corner_end_idx = data.find(lower_corner_end, cur-lower_corner_end_length)
            cur += size
        if lower_corner_end_idx != -1:
            upper_corner_end_idx = data.find(upper_corner_end, lower_corner_end_idx+1)
            if upper_corner_end_idx == -1:
                data += f.read(size)
                upper_corner_end_idx = data.find(upper_corner_end, lower_corner_end_idx+1)
    if upper_corner_end_idx != -1:
        lower_corner_start_idx = data.rfind('>', 0, lower_corner_end_idx)+1
        upper_corner_start_idx = data.rfind('>', 0, upper_corner_end_idx)+1
        b, l = (lc := data[lower_corner_start_idx:lower_corner_end_idx].split(' '))[0], lc[1]
        t, r = (uc := data[upper_corner_start_idx:upper_corner_end_idx].split(' '))[0], uc[1]
        citygml_bbox = f"POLYGON(({l} {b}, {l} {t}, {r} {t}, {r} {b}, {l} {b}))"
    else:
        citygml_bbox = ''
    return citygml_bbox


def match_to_estate_id():
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                   os.environ["DBNAME"], os.environ["USER"],
                                                                   os.environ["PASSWORD"])) as conn:
        print("*** オープンデータでマッチング ***")
        # ファイル・セッションID・ユーザID毎にマッチング処理を行い、データを格納する
        for file in os.listdir(input_dir):
            if file.endswith('.gml'):
                estate_id_user_id = os.environ.get('ESTATE_ID_USER_ID')
                estate_id_session_id = os.environ.get('ESTATE_ID_SESSION_ID')

                print(f"file: {file}")
                create_sql = f'''
                INSERT INTO building_citygml_matched
                SELECT
                b.gml_id,b.建物id,b.lod0geom,b.filename,b.user_id,b.session_id,
                COALESCE(fim.tatemono_id, '') as tatemono_id,
                bm.bldg_id,
                bm.bunrui,bm.n_touki,bm.floor_space,bm.structure_code,
                COALESCE(b.measuredheight, 0) as height,
                bm.floors,bm.region,
                COALESCE(fim.tochi_id, '') as fudosan_id,
                '' AS algorithm_flag,
                0 as score_fude,
                0 as score_high,
                0 as score_wide,
                0 as score_total,
                COALESCE(b.storeysAboveGround, 0) as citygml_floors,
                COALESCE(b.storeysBelowGround, 0) as citygml_floors_below_ground,
                COALESCE(b.buildingFootprintArea_uro, 0) as citygml_floor_space,
                COALESCE(b.usage, 0) as citygml_usage_code,
                COALESCE(b.buildingStructureType_uro, 0) as citygml_structure_code,
                COALESCE(b.yearOfConstruction, 0) as yearOfConstruction,
                COALESCE(bm.floors, 0) as storeysAboveGround,
                COALESCE(bm.floors_below_ground, 0) as storeysBelowGround,
                COALESCE(bm.floor_space, 0) as buildingFootprintArea,
                COALESCE(bm.usage_code, 0) as usage,
                COALESCE(bm.structure_code, 0) as buildingStructureType_uro,
                COALESCE(bm.construction_year, 0) as yearOfConstruction
                FROM building_citygml b
                JOIN building_master bm ON ST_Intersects(bm.region, b.lod0geom)
                join propertyid_master pm on pm.bldg_id = bm.bldg_id
                join full_id_master as fim ON  fim.bldg_id = bm.bldg_id
                WHERE
                b.filename = '{file}'
                AND b.user_id = '{estate_id_user_id}'
                AND b.session_id = '{estate_id_session_id}'
                '''
                citygml_bbox = get_citygml_bbox(os.path.join(input_dir, file))
                if citygml_bbox:
                    create_sql += f"\nAND ST_Intersects(ST_GeometryFromText('{citygml_bbox}', 4326), bm.region)"
                conn.cursor().execute(create_sql)

                # マッチングデータ追加件数チェック用SQL
                count_sql = f'''
                SELECT count(*) AS row_count
                FROM building_citygml_matched
                WHERE filename = '{file}'
                AND user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND algorithm_flag = ''
                '''
                cursor = conn.cursor()
                cursor.execute(count_sql)
                row = cursor.fetchall()
                len_matched = len(row)
                if len_matched > 0:
                    print(f"マッチングデータ追加件数: {row[0][0]}件")
                else:
                    print("マッチングデータ追加件数: 0件")

def delete_working_table_data():
    print("delete building_citygml_matched, building_citygml table data.")
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                   os.environ["DBNAME"], os.environ["USER"],
                                                                   os.environ["PASSWORD"])) as conn:
        estate_id_user_id = os.environ.get('ESTATE_ID_USER_ID')
        estate_id_session_id = os.environ.get('ESTATE_ID_SESSION_ID')

        cursor = conn.cursor()
        delete_sql = f'''
            DELETE FROM building_citygml_matched
            WHERE
            user_id = '{estate_id_user_id}'
            AND session_id = '{estate_id_session_id}'
        '''
        cursor.execute(delete_sql)

        delete_sql = f'''
            DELETE FROM building_citygml
            WHERE
            user_id = '{estate_id_user_id}'
            AND session_id = '{estate_id_session_id}'
        '''
        cursor.execute(delete_sql)
    return True



def calc_algorithm_flag():
    """特にオープンデータでマッチングしたデータの値を確認し、アルゴリズムフラグを設定する"""
    # return True

    # print("*** アルゴリズムフラグを設定 ***")
    # # ファイル・セッションID・ユーザID毎にマッチング処理を行い、データを格納する

    # ファイル・セッションID・ユーザID毎にマッチング処理を行い、データを格納する
    print("calc_algorithm_flag:")
    estate_id_user_id = os.environ.get('ESTATE_ID_USER_ID')
    estate_id_session_id = os.environ.get('ESTATE_ID_SESSION_ID')
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(
                os.environ["HOST"], os.environ["PORT"],
                os.environ["DBNAME"], os.environ["USER"],
                os.environ["PASSWORD"])) as conn:

        for file in os.listdir(input_dir):
            if file.endswith('.gml'):
                print(f"file: {file}")

                # スコアの設定 score_fude
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_fude = ROUND(100 * ST_Area(ST_Intersection(lod0geom, region)) / ST_Area(lod0geom))
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                '''
                conn.cursor().execute(update_sql)

                # score_fude が NULL のレコードについて、score_high = 0 に設定する
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_fude = 0
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND score_fude IS NULL
                '''
                conn.cursor().execute(update_sql)


                # スコアの設定 score_high
                # citygmlの地上階数・地下階数が登記データの地上階数・地下階数と一致してる場合、
                # score_high = 100 に設定する。
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_high = 100
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND citygml_floors = storeysAboveGround
                AND citygml_floors_below_ground = storeysBelowGround
                '''
                conn.cursor().execute(update_sql)

                # その他 score_high の設定
                # 登記データの地上階数・地下階数とPLATEAU階数が一致してたら、100点
                # 一致してない場合、
                # 100-ABS(登記データの階数 * 2.85m + 1.93m - PLATEAU 建物の高さ)
                high_value = 2.85
                minus_high_value = 1.93

                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_high =
                CASE WHEN ((100 - abs(NULLIF(floors, 0) * {high_value} + {minus_high_value} / NULLIF(floors, 0)))) < 0 THEN 0
                ELSE ((100 - abs(NULLIF(floors, 0) * {high_value} + {minus_high_value} / NULLIF(floors, 0))))
                END
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND score_high = 0
                '''
                conn.cursor().execute(update_sql)

                # score_high が NULL のレコードについて、score_high = 0 に設定する
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_high = 0
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND score_high IS NULL
                '''
                conn.cursor().execute(update_sql)

                # スコアの設定 score_wide
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_wide = 100
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND citygml_floor_space = buildingFootprintArea
                '''
                conn.cursor().execute(update_sql)

                # 登記データの床面積が、PLATEAU footPrintArea とm2単位で一致していたら、100点
                # 一致してない場合、
                # 100 - (ABS(登記データの1F床面積 - PLATEAU 建物の図形の面積 * 0.8) / 登記データの1F床面積) * 100
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_wide = CASE when floor_space = 0 THEN 0
                when (100 - abs(floor_space - ST_Area(lod0geom::geography) * 0.8) / NULLIF(floor_space, 0) * 100) < 0 THEN 0
                ELSE (100 - abs(floor_space - ST_Area(lod0geom::geography) * 0.8) / NULLIF(floor_space, 0) * 100)
                END
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND score_wide = 0
                '''
                conn.cursor().execute(update_sql)

                # score_wide が NULL のレコードについて、score_high = 0 に設定する
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_wide = 0
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND score_wide IS NULL
                '''
                conn.cursor().execute(update_sql)

                # 各行のスコアの合計値を算出
                update_sql = f'''
                UPDATE building_citygml_matched
                SET score_total = ((score_fude + score_high + score_wide) / 3)
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                '''
                conn.cursor().execute(update_sql)

                # 件数確認用SQL
                count_sql = f'''
                SELECT count(*) AS row_count
                FROM building_citygml_matched
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                '''
                cursor = conn.cursor()
                cursor.execute(count_sql)
                row = cursor.fetchall()
                len_matched = len(row)
                if len_matched > 0:
                    print(f"削除前件数: {row[0][0]}件")

                # この時点でスコア合計値が50点未満のレコードは削除(残しておくことで誤マッチングの可能性があるため)
                legcut_score = 50
                update_sql = f'''
                DELETE FROM building_citygml_matched
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                AND score_total < {legcut_score}
                '''
                conn.cursor().execute(update_sql)

                # create uuid from estate_id_user_id and estate_id_session_id
                temporary_table_name = 'building_citygml_matched_tmp'

                # create temporary table
                create_sql = f'''
                CREATE TEMPORARY TABLE IF NOT exists {temporary_table_name} (
                    gml_id varchar NOT NULL,
                    建物id varchar(16) NULL,
                    lod0geom public.geometry(geometry, 4326) NULL,
                    filename varchar(255) NULL,
                    user_id varchar(255) NOT NULL,
                    session_id varchar(255) NOT NULL,
                    tatemono_id text NULL,
                    bldg_id varchar(18) NOT NULL,
                    bunrui varchar(8) NULL,
                    n_touki integer NULL,
                    floor_space float4 NULL,
                    structure_code integer NULL,
                    height double precision NULL,
                    floors integer NULL,
                    region public.geometry(multipolygon, 4326) NULL,
                    fudosan_id text NOT NULL,
                    algorithm_flag varchar(2) NULL,
                    score_fude integer NULL,
                    score_high integer NULL,
                    score_wide integer NULL,
                    score_total integer NULL,
                    score_total_max integer NULL,
                    matching_count integer NULL DEFAULT 0,

                    citygml_floors integer NULL,
                    citygml_floors_below_ground integer NULL,
                    citygml_floor_space float4 NULL,
                    citygml_usage_code integer NULL,
                    citygml_structure_code integer NULL,
                    citygml_construction_year integer NULL,

                    storeysAboveGround integer NULL,
                    storeysBelowGround integer NULL,
                    buildingFootprintArea float4 NULL,
                    usage integer NULL,
                    buildingStructureType_uro integer NULL,
                    yearOfConstruction integer NULL,
                    fudosan_id_hash varchar(32) NULL
                );
                CREATE INDEX IF NOT exists building_citygml_matched_idx1 ON {temporary_table_name} (gml_id, filename, user_id, session_id);
                CREATE INDEX IF NOT exists building_citygml_matched_idx2 ON {temporary_table_name} (gml_id, user_id, session_id);
                CREATE INDEX IF NOT exists building_citygml_matched_idx3 ON {temporary_table_name} (gml_id);
                CREATE INDEX IF NOT exists building_citygml_matched_idx4 ON {temporary_table_name} (algorithm_flag);
                '''
                conn.cursor().execute(create_sql)

                # insert data to temporary table
                insert_sql = f'''
                INSERT INTO {temporary_table_name}
                SELECT
                *
                FROM building_citygml_matched
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                '''
                conn.cursor().execute(insert_sql)

                # matching function on temporary table
                # delete data from building_citygml_matched
                delete_sql = f'''
                DELETE FROM building_citygml_matched
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                '''
                conn.cursor().execute(delete_sql)

                # matching function on temporary table
                # 件数確認用SQL
                count_sql = f'''
                SELECT count(*) AS row_count
                FROM {temporary_table_name}
                '''
                cursor = conn.cursor()
                cursor.execute(count_sql)
                row = cursor.fetchall()
                len_matched = len(row)
                if len_matched > 0:
                    print(f"一時テーブル追加件数: {row[0][0]}件")

                # 建物不動産ID, 建物ID, 不動産IDの連結文字列を元にハッシュ値を作って更新する
                # alter table {temporary_table_name} add column fudosan_id_hash varchar(32);
                update_sql = f'''
                update {temporary_table_name} set fudosan_id_hash = md5(tatemono_id||bldg_id||fudosan_id);
                '''
                conn.cursor().execute(update_sql)

                # インデックスを定義する
                create_sql = f'''
                CREATE INDEX IF NOT exists building_citygml_matched_idx5 ON {temporary_table_name} (gml_id, fudosan_id_hash, score_total);
                CREATE INDEX IF NOT exists building_citygml_matched_idx6 ON {temporary_table_name} (gml_id, fudosan_id_hash);
                '''
                conn.cursor().execute(create_sql)

                # ウィンドウ関数を利用してランキング1位のレコードにalgorithm_flag = 1 を設定する
                update_sql = f'''
                UPDATE {temporary_table_name} as a
                set algorithm_flag = '1',
                score_total_max = subq.score_total
                FROM (
                    SELECT gml_id, fudosan_id_hash, score_total
                    FROM (
                        SELECT
                        gml_id, fudosan_id_hash, score_total,
                        rank() over (partition by gml_id ORDER BY score_total desc) AS score_rank
                        FROM {temporary_table_name} bcm
                        group by gml_id, fudosan_id_hash, score_total
                    ) as b
                    where b.score_rank = 1
                ) as subq
                WHERE
                a.gml_id = subq.gml_id
                and a.fudosan_id_hash = subq.fudosan_id_hash
                '''
                conn.cursor().execute(update_sql)

                # 点数が最高点+-5点であるレコードについて、algorithm_flag = 10 に設定する
                update_sql = f'''
                UPDATE {temporary_table_name}
                SET algorithm_flag = '10'
                WHERE
                gml_id in (
                    SELECT gml_id
                    FROM (
                        SELECT gml_id, count(gml_id) as count_gml_id
                        FROM {temporary_table_name}
                        WHERE score_total BETWEEN (score_total_max - 5) AND (score_total_max + 5)
                        GROUP BY gml_id
                    ) AS a
                    WHERE count_gml_id > 1
                )
                '''
                conn.cursor().execute(update_sql)

                # 同点1位のlgorithm_flag = 10 のレコードを他の条件でチェック。
                # 建築年が+-1であるか
                # 該当するレコードがあれば、matching_countを増やす
                update_sql = f'''
                UPDATE {temporary_table_name}
                SET matching_count = matching_count + 1
                WHERE
                algorithm_flag = '10'
                AND citygml_construction_year BETWEEN (yearOfConstruction-1) AND (yearOfConstruction+1)
                '''
                conn.cursor().execute(update_sql)

                # 同点1位のlgorithm_flag = 10 のレコードを他の条件でチェック。
                # 構造が同じかどうか
                # 該当するレコードがあれば、matching_countを増やす
                update_sql = f'''
                UPDATE {temporary_table_name}
                SET matching_count = matching_count + 1
                WHERE
                algorithm_flag = '10'
                AND citygml_structure_code = buildingStructureType_uro
                '''
                conn.cursor().execute(update_sql)

                # 同点1位のlgorithm_flag = 10 のレコードを他の条件でチェック。
                # 用途が同じか
                # 該当するレコードがあれば、matching_countを増やす
                update_sql = f'''
                UPDATE {temporary_table_name}
                SET matching_count = matching_count + 1
                WHERE
                algorithm_flag = '10'
                AND citygml_usage_code = usage
                '''
                conn.cursor().execute(update_sql)

                # matching_count が最も高いレコードについて、algorithm_flag = 1 に設定する
                # gml_idが複数発生するので、この条件は不要
                # update_sql = f'''
                # UPDATE {temporary_table_name} as a
                # set algorithm_flag = '1'
                # FROM (
                #     SELECT gml_id, fudosan_id_hash, matching_count
                #     FROM (
                #         SELECT
                #         gml_id, fudosan_id_hash, score_total, matching_count,
                #         rank() over (partition by gml_id ORDER BY matching_count desc) AS score_rank
                #         FROM {temporary_table_name} bcm
                #         WHERE algorithm_flag = '10'
                #         group by gml_id, fudosan_id_hash, score_total, matching_count
                #     ) as b
                #     where b.score_rank = 1 and matching_count > 0
                # ) as subq
                # where a.gml_id = subq.gml_id
                # and a.fudosan_id_hash = subq.fudosan_id_hash
                # '''
                # conn.cursor().execute(update_sql)

                # matching_count, score_total が最も高いレコードについて、algorithm_flag = 1 に設定する
                update_sql = f'''
                UPDATE {temporary_table_name} as a
                set algorithm_flag = '1'
                FROM (
                    SELECT gml_id, fudosan_id_hash, score_total, matching_count
                    FROM (
                        SELECT
                        gml_id, fudosan_id_hash, score_total, matching_count,
                        rank() over (partition by gml_id ORDER BY matching_count desc, score_total desc) AS score_rank
                        FROM {temporary_table_name} bcm
                        WHERE algorithm_flag = '10'
                        group by gml_id, fudosan_id_hash, score_total, matching_count
                    ) as b
                    where b.score_rank = 1 and matching_count > 0
                ) as subq
                where a.gml_id = subq.gml_id
                and a.fudosan_id_hash = subq.fudosan_id_hash
                and a.score_total = subq.score_total
                '''
                conn.cursor().execute(update_sql)

                # 敷地に含まれるため同じ建物不動産IDが設定される敷地内の複数の建物について、
                # 最もマッチングスコアが大きなものに対してだけ、algorithm_flag = 1 に設定するための処理
                update_sql = f'''
                UPDATE {temporary_table_name} as a
                set algorithm_flag = '10'
                FROM (
                    SELECT gml_id, bldg_id, fudosan_id_hash, score_total
                    FROM (
                        SELECT
                        gml_id, bldg_id, fudosan_id_hash, score_total,
                        rank() over (partition by bldg_id ORDER BY score_total desc) AS score_rank
                        FROM {temporary_table_name} bcm
                        WHERE algorithm_flag = '1'
                        GROUP by gml_id, bldg_id, fudosan_id_hash, score_total
                    ) as b
                    WHERE b.score_rank > 1
                ) as subq
                WHERE a.gml_id = subq.gml_id
                and a.fudosan_id_hash = subq.fudosan_id_hash
                '''
                conn.cursor().execute(update_sql)

                # algorithm_flag = 1以外のレコードを削除する
                delete_sql = f'''
                DELETE FROM {temporary_table_name}
                WHERE
                algorithm_flag <> '1'
                '''
                conn.cursor().execute(delete_sql)

                # 不動産idから生成したハッシュ値を削除する
                update_sql = f'''
                ALTER TABLE {temporary_table_name} DROP COLUMN fudosan_id_hash;
                '''
                # conn.cursor().execute(update_sql)

                # building_citygml_matched にデータを戻す
                insert_sql = f'''
                INSERT INTO building_citygml_matched
                SELECT
                DISTINCT
                *
                FROM {temporary_table_name}
                '''
                conn.cursor().execute(insert_sql)

                # drop temporary table
                drop_sql = f'''
                DROP TABLE {temporary_table_name}
                '''
                conn.cursor().execute(drop_sql)

                # 件数チェック
                count_sql = f'''
                SELECT count(*) AS row_count
                FROM building_citygml_matched
                WHERE
                user_id = '{estate_id_user_id}'
                AND session_id = '{estate_id_session_id}'
                AND filename = '{file}'
                '''
                cursor = conn.cursor()
                cursor.execute(count_sql)
                row = cursor.fetchall()
                len_matched = len(row)
                if len_matched > 0:
                    print(f"不動産ID付与件数: {row[0][0]}件")

    return True


def append_new_elements(parent: etree._Element, tag_order_list: list, uro_uri: str):
    """parentに新しいstringAttribute型の子要素とその値を追加する"""
    # 不動産ID用のクラス要素を追加
    class_element_main = etree.Element(etree.QName(uro_uri, "bldgRealEstateIDAttribute"))
    class_element_sub  = etree.Element(etree.QName(uro_uri, "RealEstateIDAttribute"))

    # 名前空間を登録
    etree.register_namespace('uro', uro_uri)
    for tag in tag_order_list:
        child_name = tag["name"]
        value = tag["value"]

        # 新しい要素を作成
        new_element = etree.Element(etree.QName(uro_uri, child_name))
        new_element.text = value
        # parent.insert(target_index, new_element)
        class_element_sub.append(new_element)

    # 新しい要素を追加
    class_element_main.append(class_element_sub)
    parent.append(class_element_main)

    return True


def add_estate_id_to_gml():
    """
    マッチング結果格納テーブルの結果を元にCityGMLファイルに不動産IDなどを付与し、
    S3バケットにアップロードする
    """
    # 出力先のフォルダを作成
    now = datetime.datetime.now()
    folder_name = now.strftime('%Y%m%d%H%M%S')
    os.makedirs(os.path.join(output_dir, folder_name), exist_ok=True)

    estate_id_user_id = os.environ.get('ESTATE_ID_USER_ID')
    estate_id_session_id = os.environ.get('ESTATE_ID_SESSION_ID')

    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(
                os.environ["HOST"], os.environ["PORT"],
                os.environ["DBNAME"], os.environ["USER"],
                os.environ["PASSWORD"])) as conn:
        for file in os.listdir(input_dir):
            if file.endswith('.gml'):
                print(f"{file}にマッチング結果を付与...")

                result_rows = []
                # マッチング情報を building_citygml_matched テーブルから取得
                select_sql = f'''
                SELECT
                    gml_id,
                    tatemono_id,
                    fudosan_id,
                    bunrui,
                    bldg_id,
                    algorithm_flag,
                    score_fude,
                    score_high,
                    score_wide,
                    CEILING(score_total) as score_total,
                    score_total_max,
                    fudosan_id_hash
                FROM building_citygml_matched as p
                WHERE
                p.user_id = '{estate_id_user_id}'
                AND p.session_id = '{estate_id_session_id}'
                AND p.filename = '{file}'
                ORDER BY gml_id, score_total DESC
                '''
                cursor = conn.cursor()
                cursor.execute(select_sql)
                result_rows = cursor.fetchall()

            len_matched = len(result_rows)
            # CityGMLファイルを読み込む
            tree = etree.parse(os.path.join(input_dir, file))
            root = tree.getroot()

            # CityGMLファイルの名前空間を取得。
            # CityGMLファイルごとに定義が異なることがあるuroの名前空間URIを取得する
            uro_uri = namespaces["uro"]
            if root.nsmap is not None:
                uro_uri = root.nsmap["uro"]
            print("uro_uri", uro_uri)

            if len_matched > 0:

                search_ids = []
                search_ids = list(map(lambda x: x[0], result_rows))
                # print(search_ids)
                # print(result_rows)
                matching_counter = 0
                for building in root.findall('.//bldg:Building', namespaces):
                    search_row = []
                    for search_id in search_ids:
                        # ファイル内の建物IDを検索
                        if building.attrib['{http://www.opengis.net/gml}id'] == search_id:
                            search_row = list(filter(lambda x: x[0] == search_id, result_rows))
                            break
                    if len(search_row) > 0:
                        # 建物不動産ID
                        tag_order_list = []
                        record = search_row[0]
                        tatemono_id = record[1]
                        tatemono_id = tatemono_id.split(",")[0]
                        obj = {
                            "name": "realEstateIDOfBuilding",
                            "type": "string",
                            "value": tatemono_id,
                        }
                        tag_order_list.append(obj)

                        # 区分所有建物ID
                        bunrui = str(record[3])
                        bldg_id = str(record[4])
                        if bunrui == '区建':
                            result = get_kubun_tatemono_id_list(bldg_id)
                            kubun_tatemono_id = result[0]
                            kubun_tatemono_count = result[1]
                            if kubun_tatemono_count > 0:
                                obj = {
                                    "name": "numberOfBuildingUnitOwnership",
                                    "type": "integer",
                                    "value": str(kubun_tatemono_count),
                                }
                                tag_order_list.append(obj)

                                for fid in kubun_tatemono_id.split(","):
                                    obj = {
                                        "name": "realEstateIDOfBuildingUnitOwnership",
                                        "type": "string",
                                        "value": fid.strip(),
                                    }
                                    tag_order_list.append(obj)

                        # 土地不動産ID
                        tochi_fudosan_id = record[2]
                        tochi_fudosan_id_len = str(len(tochi_fudosan_id.split(",")))
                        obj = {
                            "name": "numberOfRealEstateIDOfLand",
                            "type": "integer",
                            "value": tochi_fudosan_id_len
                        }
                        tag_order_list.append(obj)

                        for fid in tochi_fudosan_id.split(","):
                            obj = {
                                "name": "realEstateIDOfLand",
                                "type": "string",
                                "value": fid.strip(),
                            }
                            tag_order_list.append(obj)

                        # その他、スコアを記録
                        score_total = int(record[9])
                        obj = {
                            "name": "matchingScore",
                            "type": "integer",
                            "value": str(score_total),
                        }
                        tag_order_list.append(obj)

                        # print(tag_order_list)
                        append_new_elements(building, tag_order_list, uro_uri)
                        matching_counter += 1

                print(f"マッチングデータ追加件数: {matching_counter}件")
            else:
                print("マッチングデータ追加件数: 0件")

            # citygmlファイルを出力
            etree.indent(tree, space="\t")
            tree.write(
                os.path.join(output_dir, folder_name, file),
                pretty_print=True,
                xml_declaration=True,
                encoding="utf-8"
                )

    # テーブルを削除
    delete_working_table_data()

    # 出力のgmlファイルをZIP化する
    shutil.make_archive(os.path.join(output_dir, folder_name), 'zip', os.path.join(output_dir, folder_name))

    no_use_iam_mode = int(os.environ.get('NO_USE_IAM_MODE'))

    # zipファイルをS3にアップロードする
    if no_use_iam_mode == 0:
        upload_res = upload_to_s3(os.path.join(output_dir, folder_name + ".zip"))
        if upload_res:
            send_complete_mail()

    return True

def get_kubun_tatemono_id_list(bldg_id):
    kubun_tatemono_id = ""
    kubun_tatemono_count = 0
    with psycopg2.connect(
            "host={} port={} dbname={} user={} password={}".format(os.environ["HOST"], os.environ["PORT"],
                                                                    os.environ["DBNAME"], os.environ["USER"],
                                                                    os.environ["PASSWORD"])) as conn:
        cursor = conn.cursor()
        cond = bldg_id
        cond = f"'{cond}'"
        cursor.execute(
            f"""SELECT
            fudosan_id, fudosan_bango, bldg_id,
            bldg_number, room_number
            FROM propertyid_master
            WHERE bldg_id IN ({cond})
            ORDER BY fudosan_bango
            """)
        rows = cursor.fetchall()
        if len(rows) > 0:
            kubun_tatemono_id = ", ".join(list(map(lambda x: x[0], rows)))
            kubun_tatemono_count = len(rows)

    return kubun_tatemono_id, kubun_tatemono_count

def upload_to_s3(file_path: str):
    print(f"{file_path}をS3にアップロード...")
    s3_client = boto3.client(service_name="s3")
    try:
        s3_client.upload_file(file_path, os.environ["BUCKET_NAME"], file_path)
    except:
        return False
    print("s3バケットへのアップロードが完了しました")
    return True


def send_complete_mail():
    def send_email(to_email_address, subject, body):
        ses_client = boto3.client("ses", region_name="ap-northeast-1")
        source_mail_address = os.environ["SES_SOURCE_EMAIL_ADDRESS"]

        response = ses_client.send_email(
            Source=source_mail_address,
            Destination={
                "ToAddresses": [
                    to_email_address,
                ]
            },
            Message={
                "Subject": {
                    "Data": subject,
                },
                "Body": {
                    "Html": {
                        "Data": body,
                    },
                },
            },
        )
        return response

    def get_cognito_mail_address(user_id):
        # cognito user pool search
        cognito_client = boto3.client("cognito-idp")

        users = cognito_client.list_users(
            UserPoolId=os.environ["USER_POOL_ID"],
            Filter=f'sub = "{user_id}"',
        ).get("Users")

        # user not found error
        if len(users) == 0:
            return {"error": "user not found"}

        # print(users[0])
        email_address = ""
        for item in users[0]["Attributes"]:
            if item.get("Name") == "email":
                email_address = item.get("Value")

        return {"email_address": email_address}

    def list_buckets(target_obj, user_id, session_id):
        bucket = os.environ["BUCKET_NAME"]

        if target_obj not in ("input", "output"):
            return {"error": "invalid target object"}

        prefix = f"data/{target_obj}/{user_id}/{session_id}/"

        s3 = boto3.resource("s3")

        s3_bucket_obj = s3.Bucket(bucket)
        objs = s3_bucket_obj.meta.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in objs:
            return {"error": "invalid target object"}

        return objs["Contents"]

    def embed_url_in_html(url):
        html_content = f"""
        <html>
            <body>
                <p>以下のリンクをクリックして、ファイルをダウンロードしてください:</p>
                <a href="{url}">ダウンロードリンク</a>
            </body>
        </html>
        """
        return html_content

    bucket = os.environ["BUCKET_NAME"]
    expires_in = float(os.environ["SIGNED_URL_EXPIRES_IN"])

    # 指定したS3バケット output/ユーザID/セッションIDに、ZIPファイルが存在するか確認する
    zip_files_s3_obj = list_buckets(
        "output",
        os.environ.get("ESTATE_ID_USER_ID"),
        os.environ.get("ESTATE_ID_SESSION_ID"),
    )

    zip_file_key = ""
    for zip_file in zip_files_s3_obj:
        if zip_file.get("Key").endswith(".zip"):
            zip_file_key = zip_file.get("Key")
            break

    # ZIPファイルのダウンロードURLを生成する
    s3_client = boto3.client("s3")
    response = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": zip_file_key},
        ExpiresIn=expires_in,
    )

    signed_url = response
    url_link = embed_url_in_html(signed_url)

    # URLの有効期限を現在時刻から expires_in 秒後に設定して日時文字列に整形する
    japan_timezone = timezone('Asia/Tokyo')
    dt = datetime.datetime.now()
    dt1 = dt + datetime.timedelta(seconds=expires_in)
    dt1_japan = dt1.astimezone(japan_timezone)
    dt2 = dt1_japan.strftime("%Y/%m/%d %H:%M:%S")

    # get email address
    obj = get_cognito_mail_address(os.environ.get("ESTATE_ID_USER_ID"))
    if obj.get("error") is not None:
        print("ユーザ情報の取得に失敗しました")
        return

    to_email_address = obj.get("email_address")

    # send email to user
    mail_subject = "[不動産ID] GMLファイルの更新が完了しました"

    mail_body = "<p>"
    mail_body += f"ユーザID: {to_email_address} 様からご登録いただいた"
    mail_body += "CityGML ファイルについて、不動産ID更新処理が完了しました。<br/>"
    mail_body += "ダウンロードURLをクリックして、GMLファイルをまとめた"
    mail_body += "ZIPファイルのダウンロードをお願いいたします。<br/>"
    mail_body += "</p>"
    mail_body += "<p>"
    mail_body += "[ダウンロードURL]<br/>"
    mail_body += f"(有効期限: {dt2} まで)<br/>"
    mail_body += f"{url_link}"
    mail_body += "</p>"

    response = send_email(to_email_address, mail_subject, mail_body)

    print(f"response: {response}")


if __name__ == "__main__":
    main()
