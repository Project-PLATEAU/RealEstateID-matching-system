import argparse
import logging
from pathlib import Path

from lib.clean_touki import GappitsuGeocodingDictionaryCreator

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
        description="土地登記変更履歴からジオコーダ用拡張辞書ファイルを生成します。")
    parser.add_argument('--jageocoder-db-dir', help='Jageocoder 辞書ディレクトリ')
    parser.add_argument('-o', help='出力ディレクトリ（省略時はカレントディレクトリ）')
    args = parser.parse_args()

    if args.o is None:
        output_dir = "."
    else:
        output_dir = args.o

    if output_dir[-1] == '/':
        output_dir = output_dir[0:-1]

    creator = GappitsuGeocodingDictionaryCreator(
        jageocoder_db_dir=args.jageocoder_db_dir
    )

    for prefcode in range(1, 48):
        filepath = Path(output_dir) / f"{prefcode:02d}_chiban_gappitsu.txt"
        fout = open(filepath, "w")
        logger.debug(f"処理結果を '{filepath}' に出力します。")
        n = creator.create_gappitsu(code_prefix=f"{prefcode:02d}", fout=fout)
        fout.close()
        if n == 0:
            logger.debug("  1件も出力されなかったため削除します。")
            filepath.unlink()
