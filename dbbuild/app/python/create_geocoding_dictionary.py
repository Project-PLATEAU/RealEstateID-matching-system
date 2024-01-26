import argparse
import logging
from pathlib import Path

from lib.clean_touki import GeocodingDictionaryCreator

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
    parser = argparse.ArgumentParser(description="14条地図からジオコーダ用辞書ファイルを生成します。")
    parser.add_argument('--jageocoder-db-dir', help='Jageocoder 辞書ディレクトリ')
    parser.add_argument('-o', help='出力ディレクトリ（省略時はカレントディレクトリ）')
    args = parser.parse_args()

    if args.o is None:
        output_dir = "."
    else:
        output_dir = args.o

    output_dir = Path(output_dir)

    creator = GeocodingDictionaryCreator(
        jageocoder_db_dir=args.jageocoder_db_dir
    )

    target_prefs = []
    for prefcode in range(1, 48):
        path = output_dir / f"{prefcode:02d}_chiban.txt"
        with open(path, "w") as outf:
            n = creator.create_dicfile(prefcode, outf)

        if n == 0:
            path.unlink()
        else:
            logger.info(f"{path} に {n} 件出力しました。")
            target_prefs.append(f"{prefcode:02d}")

    with open(output_dir / "target_prefs.txt", "w") as f:
        codes = " ".join(target_prefs)
        print(codes, file=f)
        logger.info(f"対象都道府県コード: {codes}")
