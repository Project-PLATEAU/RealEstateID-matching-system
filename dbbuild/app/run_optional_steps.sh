#!/bin/bash

# 「不動産ID空間データ」を作成する手順以外の手順を実行するバッチスクリプト。
# 2023-10-12 版

# 実行方法
# プロジェクトルートディレクトリ (docker-compose.yml がある) にて
# poetry shell
# ROOT_DIR=`pwd` bash app/run_optional_steps.sh

# 直前の処理が成功したかどうか判定する関数
function status_check() {
    if [ $? == 0 ]; then
        echo -e "\e[32mOK\e[0m"
    else
        echo -e -n "\e[31mNG (エラーコード: $?)\e[0m"
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
# パラメータ設定は run.sh をコピーすること

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
LOGFILE=`date '+%Y%m%d_%H%M%S'`.log
# LOGFILE=/dev/null
echo "処理開始: `date`" > ${LOGFILE}

# A1. full_id_master を作成
echo -e "---------------------------------\nA1. オプション手順\n"
step=1
echo "[$step] full_id_master を作成。"
echo -e -n "\e[36m"
${PSQL} -f ${SQL_DIR}/08_create_master.sql >> ${LOGFILE}
status_check
echo -e -n "\e[0m"

# A2. plateau_answer を作成
step=`expr $step + 1`
echo "[$step] plateau_answer を作成。"
echo -e -n "\e[36m"
${PSQL} -f ${SQL_DIR}/09_plateau_matching.sql >> ${LOGFILE}
status_check
echo -e -n "\e[0m"


echo -e "---------------------------------\n完了しました。"
echo "処理終了: `date`" >> ${LOGFILE}
