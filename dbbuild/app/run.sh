#!/bin/bash

# 「不動産ID空間データ」を作成する手順を自動実行するバッチスクリプト。
# 2023-09-08 版

# 実行方法
# プロジェクトルートディレクトリ (docker-compose.yml がある) にて
# docker compose down -v
# docker compose up -d postgis_realestate
# poetry shell
# ROOT_DIR=`pwd` bash app/run.sh

# Timezone
TZ=JST-9

# ANSI color codes
RED="$(printf '\033[31m')"
BLUE="$(printf '\033[32m')"
SKYBLUE="$(printf '\033[36m')"
DEFAULT="$(printf '\033[0m')"
# RED=""
# BLUE=""
# SKYBLUE=""
# DEFAULT=""

# 直前の処理が成功したかどうか判定する関数
function status_check() {
    if [ $? == 0 ]; then
        echo -e "${BLUE}OK${DEFAULT}"
    else
        echo -e -n "${RED}NG (エラーコード: $?)${DEFAULT}"
        if [[ $1 != "ignore" ]]; then
            echo "-> 終了します。"
            exit 1
        else
            echo "-> このエラーは問題ないので無視します。"
        fi
    fi
}

# docker-compose.yml で指定している設定情報
# この値を変更する場合は docker-compose.yml の変更が必要。
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGDB=${PGDB:-pgdb}
PGUSER=${PGUSER:-pguser}
PGPASS=${PGPASS:-pgpass}
PGHOST_DOCKER=${PGHOST_DOCKER:-postgis_realestate}

PSQL="psql postgresql://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/${PGDB}"
# =====================================================
# パラメータ設定 ここから

ROOT_DIR=${ROOT_DIR:-`pwd`}
if [ ${ROOT_DIR} == "/" ]; then
    # Docker コンテナ内で実行中とみなす
    ROOT_DIR=""
    OGR2OGR="/usr/bin/ogr2ogr"
else
    # 通常シェル上で実行中とみなす
    OGR2OGR="docker compose run --rm gdal ogr2ogr"
fi

# 実行環境設定
BASE_DIR=${ROOT_DIR}/app
SQL_DIR=${BASE_DIR}/sql
PYTHON_DIR=${BASE_DIR}/python
INPUT_DIR=${ROOT_DIR}/input
WORK_DIR=${ROOT_DIR}/work
JAGEOCODER_OUTPUT_DIR=${WORK_DIR}/jageocoder_output
CUSTOM_JAGEOCODER_DIR=${WORK_DIR}/jageocoder_chiban

# パラメータ設定 ここまで

# =====================================================
START_AT=`TZ=JST-9 date '+%Y%m%d_%H%M%S'`
LOGFILE=${WORK_DIR}/${START_AT}.log
echo "処理開始: ${START_AT}" > ${LOGFILE}

# 0. 初期化
echo -e "---------------------------------\n0. 事前準備\n"
step=1
echo "[$step] テーブルを初期化。"
echo -n "${SKYBLUE}"
${PSQL} -f ${SQL_DIR}/01_init_tables.sql >> ${LOGFILE}
status_check
echo -n "${DEFAULT}"


step=`expr $step + 1`
echo "[$step] ジオコーダ用辞書データを削除。"
echo -n "${SKYBULE}"
rm -f ${JAGEOCODER_OUTPUT_DIR}/text/*_chiban*.txt >> ${LOGFILE} 2>&1
rm -r ${CUSTOM_JAGEOCODER_DIR} >> ${LOGFILE} 2>&1
status_check ignore
echo -n "${DEFAULT}"


# 1. 登記所備付地図データ登録
echo -e "---------------------------------\n1. 登記所備付地図データの登録\n"

step=1
echo "[$step] PostGIS に GeoJSON を登録。"
echo -n "${SKYBULE}"
for gz in `find ${INPUT_DIR} -name \*.geojson.gz | sort`
do
    geojson=${gz%.gz}
    geojson_basename=${geojson##*/}
    gzip -dc $gz > ${WORK_DIR}/${geojson_basename}
    docker_geojson=/work/${geojson_basename}
    echo "${geojson} をインポート。"
    ${OGR2OGR} -f "PostgreSQL" PG:"host=${PGHOST_DOCKER} port=${PGPORT} dbname=${PGDB} user=${PGUSER} password=${PGPASS}" ${docker_geojson#data} -nln "fude_original"
    rm ${WORK_DIR}/${geojson_basename}
done
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 筆コード生成・筆代表点座標を付与した筆ポリゴンテーブルを作成。"
echo -n "${SKYBULE}"
${PSQL} -f ${SQL_DIR}/02_generate_fude_data.sql >> ${LOGFILE}
status_check
echo -n "${DEFAULT}"


# 2. ジオコーダ用辞書生成
echo -e "---------------------------------\n2. ジオコーダ用辞書生成\n"

step=1
echo "[$step] 筆ポリゴンテーブルから抽出した所在地表記を正規化し、ジオコーダ用辞書データ（テキストファイル）を作成。"
echo -n "${SKYBULE}"
mkdir -p ${JAGEOCODER_OUTPUT_DIR}/text/
python3 ${PYTHON_DIR}/create_geocoding_dictionary.py -o ${JAGEOCODER_OUTPUT_DIR}/text/
status_check
target_prefs=`cat ${JAGEOCODER_OUTPUT_DIR}/text/target_prefs.txt`
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] ジオコーダ用辞書データ（バイナリファイル）に変換。"
echo -n "${SKYBULE}"
python3 -m jageocoder_converter -q --no-postcode --no-gaiku --no-geolonia --no-jusho --no-basereg --db-dir=${CUSTOM_JAGEOCODER_DIR} --output-dir=${JAGEOCODER_OUTPUT_DIR} ${target_prefs} >> ${LOGFILE} 2>&1
status_check
echo -n "${DEFAULT}"


# 3. 合筆履歴解析
echo -e "---------------------------------\n3. 合筆履歴解析\n"

step=1
echo "[$step] 土地登記データをインポート。"
echo -n "${SKYBULE}"
${PSQL} -c "TRUNCATE tochi_original;" >> ${LOGFILE}
for gz in `find ${INPUT_DIR} -name \*_tochi\*.csv.gz | sort`
do
    docker_gz=${gz#${INPUT_DIR}}
    echo -n "${gz} をインポート"
    first_line=`gzip -dc ${gz} 2> /dev/null | head -1`
    if [ ${first_line:0:18} = "org_shozai,shozai," ]; then
        echo "（ヘッダ行有り）"
        header="HEADER"
    else
        echo "（ヘッダ行無し）"
        header=""
    fi
    ${PSQL} -c "COPY tochi_original FROM PROGRAM 'gzip -dc /input${docker_gz}' DELIMITER ',' CSV ${header};"
done
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 土地登記データから所在地地番ごとの土地不動産番号の対応表を作成。"
echo -n "${SKYBULE}"
${PSQL} -f ${SQL_DIR}/03_generate_tochi_data.sql >> ${LOGFILE}
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 土地登記の合筆情報から未登録の合筆前地番を抽出し、ジオコーダ用拡張辞書データ（テキストファイル）を作成。"
echo -n "${SKYBULE}"
python3 ${PYTHON_DIR}/create_gappitsu.py --jageocoder-db-dir=${CUSTOM_JAGEOCODER_DIR} -o ${JAGEOCODER_OUTPUT_DIR}/text/
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] ジオコーダ用拡張辞書データ（バイナリファイル）に変換。"
echo -n "${SKYBULE}"
python3 -m jageocoder_converter -q --no-postcode --no-gaiku --no-geolonia --no-jusho --no-basereg --db-dir=${CUSTOM_JAGEOCODER_DIR} --output-dir=${JAGEOCODER_OUTPUT_DIR} ${target_prefs} >> ${LOGFILE} 2>&1
status_check
echo -n "${DEFAULT}"


# 4. 土地不動産ID付与
echo -e "---------------------------------\n4. 土地不動産ID付与\n"

step=1
echo "[$step] 土地登記データをジオコーディングして、土地登記の不動産番号と筆ポリゴンの対応表を作成。"
echo -n "${SKYBULE}"
mkdir -p ${WORK_DIR}
python3 ${PYTHON_DIR}/clean_touki_tochi.py --jageocoder-db-dir=${CUSTOM_JAGEOCODER_DIR} -o ${WORK_DIR}
status_check
${PSQL} -c "TRUNCATE tochi_bango;" >> ${LOGFILE}
${PSQL} -c "COPY tochi_bango FROM '/work/tochi_bango.csv' DELIMITER ',' CSV HEADER;" >> ${LOGFILE}
${PSQL} -c "CREATE INDEX idx_tochi_bango_fude_code ON tochi_bango (筆コード);" >> ${LOGFILE}
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 筆ポリゴンに土地の不動産番号から生成した不動産IDを付与。"
echo -n "${SKYBULE}"
${PSQL} -c "UPDATE fude_master SET tochi_id=t.土地id FROM tochi_bango t WHERE cd=t.筆コード;" >> ${LOGFILE}
status_check
echo -n "${DEFAULT}"


# 5. 建物データ生成
echo -e "---------------------------------\n5. 建物データ生成\n"

step=1
echo "[$step] 建物登記データをインポート。"
echo -n "${SKYBULE}"
for gz in `find ${INPUT_DIR} -name \*_tatemono\*.csv.gz | sort`
do
    docker_gz=${gz#${INPUT_DIR}}
    echo -n "${gz} をインポート"
    first_line=`gzip -dc ${gz} 2> /dev/null | head -1`
    if [ ${first_line:0:21} = "fudosan_bango,bunrui," ]; then
        echo "（ヘッダ行有り）"
        header="HEADER"
    else
        echo "（ヘッダ行無し）"
        header=""
    fi
    ${PSQL} -c "COPY tatemono_original FROM PROGRAM 'gzip -dc /input${docker_gz}' DELIMITER ',' CSV ${header};"
done
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 建物の分類に合わせて不動産IDを計算し、棟で集約して不動産ID対応データと建物データを生成。"
echo -n "${SKYBULE}"
${PSQL} -f ${SQL_DIR}/05_generate_building_data.sql >> ${LOGFILE}
status_check
echo -n "${DEFAULT}"


# 6. 建物所在地展開・筆コード付与
echo -e "---------------------------------\n6. 建物所在地展開・筆コード付与\n"

step=1
echo "[$step] 建物登記データの記載地番を展開し、全ての地番をジオコーディングして筆コードがあれば付与し、建物所在地データ(CSV)に出力。"
echo -n "${SKYBULE}"
python3 ${PYTHON_DIR}/clean_touki_tatemono.py --jageocoder-db-dir=${CUSTOM_JAGEOCODER_DIR} -o ${WORK_DIR}
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 建物所在地データ(CSV)をデータベースにインポート。"
echo -n "${SKYBULE}"
${PSQL} -c "COPY tatemono_shozaichi FROM '/work/tatemono_shozaichi.csv' DELIMITER ',' CSV HEADER;"
status_check
echo -n "${DEFAULT}"


# 7. 建物データ属性更新
echo -e "---------------------------------\n7. 建物データ属性更新\n"

step=1
echo "[$step] PLATEAU 建物と比較可能な属性を計算。"
echo -n "${SKYBULE}"
python3 ${PYTHON_DIR}/generate_plateau_attributes.py -o ${WORK_DIR}
${PSQL} -c "COPY plateau_attributes FROM '/work/plateau_attributes.csv' DELIMITER ',' CSV;"
status_check
echo -n "${DEFAULT}"

step=`expr $step + 1`
echo "[$step] 建物全体の不動産IDと PLATEAU 建物用属性を建物データに付与する。"
echo -n "${SKYBULE}"
${PSQL} -f ${SQL_DIR}/07_update_building_master.sql >> ${LOGFILE}
status_check
echo -n "${DEFAULT}"


echo -e "---------------------------------\n8. 結果確認\n"
step=1
echo "[$step] データテーブル件数の計算。"
echo -n "筆ポリゴン (fude_master) :"
${PSQL} -XAwt -c "SELECT count(*) from fude_master"
echo -n "建物データ (building_master) :"
${PSQL} -XAwt -c "SELECT count(*) from building_master"
echo -n "建物ID対応データ (propertyid_master) :"
${PSQL} -XAwt -c "SELECT count(*) from propertyid_master"
echo -n "建物所在地データ (tatemono_shozaichi) :"
${PSQL} -XAwt -c "SELECT count(*) from tatemono_shozaichi"


# 9. データベースをダンプ
echo -e "---------------------------------\n9. データベースダンプ\n"

step=1
echo "[$step] 不動産ID空間データをファイルにダンプ。"
echo -n "${SKYBULE}"
pg_dump postgresql://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/${PGDB} \
  --schema=public --no-owner --create | gzip -c \
  > ${WORK_DIR}/realestate_id_db-${START_AT}.dump.gz
status_check
echo -n "${DEFAULT}"

echo -e "---------------------------------\n完了しました。"
echo "処理終了: `TZ=${TZ} date`" >> ${LOGFILE}
chmod -R a+rw ${WORK_DIR}
