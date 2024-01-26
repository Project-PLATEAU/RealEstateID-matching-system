"""
土地登記データの「所在及び地番」フィールドを解析し、
所在地地番と14条地図の筆の1対1関連を表現する
「土地ID対応データ」を生成します。
※このデータは「不動産ID空間データ」に含まれません。

生成したデータは -o オプションで指定したディレクトリの下の
tochi_bango.csv ファイルに出力します。
"""
import argparse
import logging
from pathlib import Path
import sys

from lib.clean_touki import LandRegistryCleaner

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
    parser = argparse.ArgumentParser(
        description="土地登記の所在地に14条地図の筆コードを付与します。")
    parser.add_argument(
        '--jageocoder-db-dir',
        help='筆コードを含む Jageocoder 辞書ディレクトリ')
    parser.add_argument('-o', help='出力ディレクトリ（省略時は標準出力）')
    args = parser.parse_args()

    if args.o is None:
        fout = sys.stdout  # 出力先ファイル
    else:
        path = Path(args.o) / "tochi_bango.csv"
        fout = open(path, "w")
        logger.info(f"処理結果を '{path}' に出力します。")

    cleaner = LandRegistryCleaner(
        jageocoder_db_dir=args.jageocoder_db_dir
    )

    print(LandRegistryCleaner.get_line([
        "市区町村コード",
        "所在",
        "地番",
        "登録の日",
        "土地id",
        "筆コード",
    ]), file=fout)

    for prefcode in range(1, 48):
        n = cleaner.clean_touki_land(code_prefix=f"{prefcode:02d}", fout=fout)

    if args.o is not None:
        fout.close()
