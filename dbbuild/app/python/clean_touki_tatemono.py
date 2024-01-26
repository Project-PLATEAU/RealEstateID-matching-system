"""
建物登記データの「所在及び地番」フィールドを解析し、
建物（棟）と14条地図の筆の多対多関連を表現する
「建物所在地データ」を生成します。

生成したデータは -o オプションで指定したディレクトリの下の
tatemono_shozaichi.csv ファイルに出力します。
"""
import argparse
import logging
from pathlib import Path
import sys

from lib.clean_touki import BuildingRegistryCleaner

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # ロガーの設定
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter('%(levelname)s:%(name)s:%(lineno)s:%(message)s')
    )
    logger.addHandler(console_handler)

    # コマンドラインパーザ
    parser = argparse.ArgumentParser(description="建物登記をクリーニングします。")
    parser.add_argument(
        '--jageocoder-db-dir',
        help='筆コードを含む Jageocoder 辞書ディレクトリ')
    parser.add_argument('-o', help='出力ファイル名（省略時は標準出力）')
    args = parser.parse_args()

    if args.o is None:
        fout = sys.stdout  # 出力先ファイル
    else:
        path = Path(args.o) / "tatemono_shozaichi.csv"
        fout = open(path, "w")
        logger.info(f"処理結果を '{path}' に出力します。")

    cleaner = BuildingRegistryCleaner(
        jageocoder_db_dir=args.jageocoder_db_dir
    )

    print(cleaner.get_line([
        "bldg_id",         # 建物ID
        "chiban_seq",      # 地番連番
        "chiban",          # 建物の地番
        "address_seq",     # 住所連番
        "address",         # 地番にマッチした住所
        "city_code",       # 市区町村コード
        "fude_code",       # 筆コード（筆ポリゴンにマッチした場合）
        "lon",             # 住所の代表点経度
        "lat",             # 住所の代表点緯度
        "level",           # 住所のレベル
        "status",          # 筆コード付与ステータス
    ]), file=fout)

    for prefcode in range(1, 48):
        n = cleaner.clean_touki_building(
            code_prefix=f"{prefcode:02d}", fout=fout
        )

    if args.o is not None:
        fout.close()
