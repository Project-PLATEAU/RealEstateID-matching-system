"""
不動産ID空間データテーブル (building_master) の属性から
PLATEAU 建物と比較可能な属性を生成します。
- yuka_menseki から
    地上階数 floors, 地下階数 floors_below_ground, 床面積 floor_space
- kousei_zairyo から
    構造種別 structure_code
- shurui から
    建築物の主な使い道 usage_code
- gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji から
    建築年 constructuon_year

生成したデータは -o オプションで指定したディレクトリの下の
plateau_attributes.csv ファイルに出力します。
"""
import argparse
import csv
from io import TextIOBase
import logging
from pathlib import Path
import re
import sys
from typing import Tuple

from lib.dbman import DBManager

logger = logging.getLogger(__name__)
dbman = DBManager()


class PlateauAttributesGenerator(object):

    num_pattern = r"[0-9\.]+"
    floor_pattern = rf"(.*[0-9]+)階.*\u25a1({num_pattern})"
    wareki_pattern = r"(明治|大正|昭和|平成|令和)([0-9]{1,2}|元)年"
    wareki_bases = {  # 各年号の 0 年に対応する西暦
        "明治": 1867,
        "大正": 1911,
        "昭和": 1925,
        "平成": 1988,
        "令和": 2018
    }
    re_num_pattern = re.compile(num_pattern)
    re_floor_pattern = re.compile(floor_pattern)
    re_wareki_pattern = re.compile(wareki_pattern)
    transtable = str.maketrans("０１２３４５６７８９・", "0123456789.")

    def __init__(self):
        """
        用途コード一覧表を読み込む。
        """
        self.usage_map = {}
        mapping_path = Path(__file__).parent / "usage_codes_mapping.csv"
        with open(mapping_path, "r", newline="", encoding="cp932") as f:
            reader = csv.DictReader(f)
            # 施設種類,出現頻度,コード,用途分類
            for row in reader:
                if row["施設種類"] and row["コード"]:
                    self.usage_map[row["施設種類"]] = int(row["コード"])

    def analyze_table(
        self,
        fout: TextIOBase
    ) -> None:
        """
        building_master テーブルの原記載フィールドを解析し、
        変換結果を出力する。
        """
        writer = csv.writer(fout)

        for row in dbman.select_records(
            query=(
                "SELECT bldg_id, n_touki, n_kyotaku, yuka_menseki, kousei_zairyo, shurui, "
                "gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji AS gennin "
                "FROM building_master m"
            ),
            params=None
        ):
            # yuka_menseki から地上階数、地下階数、床面積
            floors, ug_floors, floor_space, total_floor_space = \
                self.analyze_yuka_menseki(row["yuka_menseki"] or "")

            # kousei_zairyo から構造種別
            structure_code = self.analyze_kousei_zairyo(
                row["kousei_zairyo"] or "")

            # shurui から建築物の主な使い道
            usage_code = self.analyze_shurui(
                row["shurui"] or "", row["n_touki"], row["n_kyotaku"])

            # gennin から建築年
            construction_year = self.analyze_gennin(row["gennin"] or "")

            writer.writerow([
                row["bldg_id"],
                f"{floors:d}",
                f"{ug_floors:d}",
                f"{floor_space:.2f}",
                f"{total_floor_space:.2f}",
                f"{structure_code:d}",
                f"{usage_code:d}",
                f"{construction_year:d}",
            ])

    def analyze_yuka_menseki(self, original: str) -> Tuple[int, float]:
        """
        yuka_menseki 原表記を解析し、階数（整数）、地下階数（整数）
        各階の面積のうち最大のもの（実数）、延べ床面積（実数）を返す。

        Parameters
        ----------
        original: str
            建物登記の床面積表記。

        Returns
        -------
        (int, int, float, float)
            地上階数、地下階数、面積、延べ床面積。
        """
        floors = {}
        translated = original.translate(self.transtable)
        max_floor = 0
        ug_floor = 0
        for part in translated.split('\u3000'):
            m = self.re_floor_pattern.match(part)
            if m:
                floor, space = m.group(1), m.group(2)
                for fl in floor.split('階'):
                    if fl not in floors:
                        floors[fl] = 0

                    try:
                        floors[fl] += float(space)
                    except ValueError:
                        logger.warning(
                            f"'{space}' は床面積として解釈できません。"
                        )
                        continue

                    if '1' <= fl[0] and fl[0] <= '9':
                        try:
                            max_floor = max(int(fl), max_floor)
                        except ValueError:
                            logger.warning(
                                f"'{fl}' は階数として解釈できません。"
                            )
                            continue

                    elif fl.startswith('地下'):
                        try:
                            ug_floor = max(int(fl[2:]), ug_floor)
                        except ValueError:
                            logger.warning(
                                f"'{fl}' は地下階数として解釈できません。"
                            )
                            continue

            else:
                m = self.re_num_pattern.match(part)
                if m:
                    max_floor = 1
                    floors["1"] = float(m.group(0))

        if len(floors) == 0:
            return (0, 0, 0.0, 0.0)

        max_space = max(floors.values())
        total_space = sum(floors.values())

        return (max_floor, ug_floor, max_space, total_space,)

    def analyze_kousei_zairyo(self, original: str) -> int:
        """
        kousei_zairyo 原表記を解析し、PLATEAU CityGML の
        buildingStructureType と比較可能なコードを返す。
        コードリストは BuildingDetailAttribute_buildingStructureType.xml

        Parameters
        ----------
        original: str
            建物登記の構成材料表記。

        Returns
        -------
        int
            buildingStructureType の3桁コード。
        """
        if "木造" in original:
            return 601  # 木造

        if "軽量鉄骨" in original:
            return 605  # 軽量鉄骨

        if "鉄骨" in original:
            if "鉄筋" in original:
                return 602  # 鉄骨鉄筋コンクリート
            else:
                return 604  # 鉄骨

        if "鉄筋" in original:
            return 603  # 鉄筋コンクリート

        if "コンクリ" in original or "石" in original or "ブロック" in original:
            return 606  # レンガ・コンクリートブロック・石

        return 611  # 不明

    def analyze_shurui(
        self,
        original: str,
        n_touki: int,
        n_kyotaku: int
    ) -> int:
        """
        shurui 原表記を解析し、PLATEAU CityGML の usage と比較可能な
        コードを返す。コードリストは Building_usage.xml

        Parameters
        ----------
        original: str
            建物登記の種類表記。
        n_touki: int
            この建物に含まれる登記数。
        n_kyotaku: int
            この建物に含まれる、種類が「居宅」の登記数。

        Returns
        -------
        int
            usage の3桁コード。
        """
        codes = []
        # 共同住宅判定
        if n_kyotaku >= 2:
            # 2戸以上が住宅の場合は共同住宅
            original = original.replace("居宅", "共同住宅")

        # コードのマッピング表に従って変換
        for shurui in re.split(r'[\u3000・ ,、兼]', original):
            if shurui in self.usage_map:
                codes.append(self.usage_map[shurui])
            else:
                codes.append(454)  # その他

        # 複数のコードを含む場合の処理
        is_commercial = (
            401 in codes or 402 in codes or 403 in codes or
            404 in codes or 421 in codes or 422 in codes
        )
        is_industrial = (441 in codes)

        if (411 in codes and is_commercial and n_kyotaku * 3 >= n_touki):
            # 商業施設等が含まれる住宅で、全登記数の 1/3 以上が住宅
            codes.insert(0, 413)  # 店舗等併用住宅

        if (412 in codes and is_commercial and n_kyotaku * 3 >= n_touki):
            # 商業施設等が含まれる共同住宅で、全登記数の 1/3 以上が住宅
            codes.insert(0, 414)  # 店舗等併用共同住宅

        if (411 in codes or 412 in codes) and is_industrial \
                and n_kyotaku * 3 >= n_touki:
            # 工業系施設が含まれる住宅または共同住宅
            codes.insert(0, 415)  # 作業所併用住宅

        if codes[0] == 454:
            # 先頭がその他の場合、それ以外のコードを探して先頭に移動する
            for code in codes:
                if code != 454:
                    codes.remove(code)
                    codes.insert(0, code)
                    break

        if len(codes) == 0:
            codes.append(461)  # 不明

        return codes[0]

    def analyze_gennin(self, original: str) -> int:
        """
        gennin_oyobi_sonohiduke_shudearutatemono_no_hyouji 原表記を解析し、
        PLATEAU CityGML の yearOfConstruction と比較可能な年を返す。
        「新築」以外の表記や解析不能な場合は -1 とする。

        Parameters
        ----------
        original: str
            建物登記の原因及びその日付_主である建物の表示表記。

        Returns
        -------
        int
            建築年（西暦）または -1。
        """
        if "新築" not in original:
            return -1

        translated = original.translate(self.transtable)
        m = self.re_wareki_pattern.search(translated)
        if m is None:
            return -1

        gengou, wareki_year = m.group(1), m.group(2)
        year = 1 if wareki_year == '元' else int(wareki_year)
        seireki = year + self.wareki_bases[gengou]

        return seireki


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
    parser = argparse.ArgumentParser(description="建物登記の床面積を解析します。")
    parser.add_argument('-o', help='出力ディレクトリ（省略時は標準出力）')
    args = parser.parse_args()

    fa = PlateauAttributesGenerator()
    if args.o is None:
        fa.analyze_table(sys.stdout)
    else:
        path = Path(args.o) / "plateau_attributes.csv"
        logger.info(f"処理結果を '{path}' に出力します。")
        with open(path, "w", newline="") as f:
            fa.analyze_table(f)
