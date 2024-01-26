"""
不動産登記データをクリーニングするクラスライブラリ。

- 不動産ID空間データの建物マスター、所在及び地番フィールドに
  含まれる複数の地番を展開し、それぞれの地番所在地を
  ジオコーディングして筆コードを紐づけます。
- 土地ID対応データテーブルに含まれる所在と地番フィールドを
  ジオコーディングして筆コードを紐づけます。
"""

from functools import lru_cache
from io import TextIOBase
import logging
from pathlib import Path
import re
import sys
from typing import Optional, List, Tuple

import jageocoder
from jageocoder.address import AddressLevel
import tqdm

from .dbman import DBManager

logger = logging.getLogger(__name__)
dbman = DBManager()


class ChibanUtil(object):
    """
    地番データを処理するクラスで共通に利用する
    関数群を集めたベースクラス。
    """

    prefcodes = {
        "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県",
        "05": "秋田県", "06": "山形県", "07": "福島県", "08": "茨城県",
        "09": "栃木県", "10": "群馬県", "11": "埼玉県", "12": "千葉県",
        "13": "東京都", "14": "神奈川県", "15": "新潟県", "16": "富山県",
        "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
        "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県",
        "25": "滋賀県", "26": "京都府", "27": "大阪府", "28": "兵庫県",
        "29": "奈良県", "30": "和歌山県", "31": "鳥取県", "32": "島根県",
        "33": "岡山県", "34": "広島県", "35": "山口県", "36": "徳島県",
        "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
        "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県",
        "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県",
    }

    trans_kansuji = str.maketrans('壱弐参', '一二三')
    re_utf16 = re.compile(r'<([0-9a-fA-F]+)>')

    def __init__(self, jageocoder_db_dir: Optional[Path] = None) -> None:
        """
        クリーナを初期化します。

        Parameters
        ----------
        jageocoder_db_dir: Path, optional
            Jageocoder の辞書ディレクトリを指定します。
            省略した場合、 jageocoder のデフォルト値を利用します。
        """
        self.jageocoder_db_dir = jageocoder_db_dir
        if self.jageocoder_db_dir is not None:
            jageocoder.init(db_dir=str(self.jageocoder_db_dir))
        else:
            jageocoder.init()

        self.tree = jageocoder.get_module_tree()

    @classmethod
    def get_line(cls, columns: list) -> str:
        """
        出力する項目のリストを取得し、文字列に整形して返します。
        """
        return ",".join([str(x) for x in columns])

    def get_pref(self, code: str) -> str:
        """
        都道府県コードから対応する都道府県名を取得します。

        Notes
        -----
        - 見つからない場合は空文字列を返します。
        """
        nodes = self.tree.search_nodes_by_codes(
            category="jisx0401",
            value=code[0:2]
        )
        for node in nodes:
            if node.name[-1] in ('都道府県'):
                return node.name

        return ''

    def get_pref_city(self, code: str) -> List[Tuple[int, str]]:
        """
        市区町村コード(5桁)から市区町村ノードを検索し、
        都道府県・郡・市区町村の住所要素リストを返します。
        """
        if code == '14209':    # 政令指定前の相模原市
            code = '14150'     # 政令指定後の相模原市
        elif code == '27127':  # 合併後の大阪市北区
            code = '27101'     # 合併前の大阪市北区
        elif code == '43201':  # 政令指定前の熊本市
            code = '43100'     # 政令指定後の熊本市

        nodes = self.tree.search_nodes_by_codes(
            category="jisx0402",
            value=code[0:5]
        )
        if len(nodes) == 0:
            return []

        names = []
        for node in nodes:
            if 'postcode' in node.note:
                break
        else:
            node = nodes[0]
            if len(nodes) > 1:
                logger.warning((
                    f"{code} の市町村名を特定できませんでした。"
                    "候補: {}".format(",".join([n.name for n in nodes]))
                ))

        for element_list in node.get_nodes_by_level():
            if element_list is not None:
                if isinstance(element_list, list):
                    # v2.1 or later
                    for e in element_list:
                        names.append(tuple([e.level, e.name]))
                else:
                    e = element_list
                    names.append(tuple([e.level, e.name]))

        return names

    def analyze_chiban(
        self,
        chiban: str,
        names: List[Tuple[int, str]]
    ) -> List[str]:
        r"""
        地番の表記を筆ごとに分解した文字列のリストを返します。

        Parameters
        ----------
        chiban: str
            解析したい地番の表記文字列。
        names: List[Tuple[int, str]]
            この地番を含む市区町村の上位住所要素のリスト。

        Returns
        -------
        List[str]
            地番住所文字列のリスト。市区町村名も補間します。

        Notes
        -----
        Step1. 全角空白で区切られた字をまたぐ表記を展開
        - 「○○地△」は「○○番地△」とみなす処理も行う。
        例：
        "日田市大字田島字畑江　５８３番地８　日田市大字北豆田字平ノ下　１４５２番地７"
        -> ["日田市大字田島字畑江　５８３番地８",
            "日田市大字北豆田字平ノ下　１４５２番地７"]

        Step2. "、”で区切られた複数地番表記を展開
        例：
        "日田市大字西有田字下スダリ　４５２番地４、４４７番地１４"
        -> ["日田市大字西有田字下スダリ４５２番地４",
            "日田市大字西有田字下スダリ４４７番地１４"]

        - 地番部分の旧漢数字（壱弐参）は新漢数字（一二三）に変換します。
        """  # noqa: E500
        chiban_list = []
        aza = None
        pref = [names[0][1]] if len(names) > 0 else None
        chiban_split = chiban.split("\u3000")
        for i, substr in enumerate(chiban_split):
            new_substr = re.sub(r'^([０-９]+)地*([０-９]*)$',
                                r'\g<1>番地\g<2>', substr)
            if new_substr != substr:
                logger.warning('"地"を"番地"に置換：{} -> {} / {}'.format(
                    substr, new_substr, chiban))
                substr = new_substr

            for name in names:
                if substr.startswith(name[1]):  # 市区町村名から始まる文字列
                    aza = substr
                    break

            else:
                if "番" not in substr:
                    # 他の自治体の住所が含まれている場合がある
                    if i == len(chiban_split) - 1 or substr.startswith("建物"):
                        # logger.warning(f"建物名？ '{substr}' / '{chiban}'")
                        continue

                    self.tree.set_config(target_area=pref)
                    results = self.tree.searchNode(substr)
                    if len(results) > 0 and len(results[0].matched) >= 3 \
                            and results[0].node.level >= 5:
                        # logger.warning(f"他の自治体？ '{substr}' / '{chiban}'")
                        aza = "{}/{}/{}".format(
                            results[0].node.get_pref_name(),
                            results[0].node.get_city_name(),
                            substr)
                        continue

                    # logger.warning(f"建物名？ '{substr}' / '{chiban}'")
                    continue

                if aza is None:
                    # logger.warning(f"字無し？ '{substr}' / '{chiban}'")
                    continue

                # 市区町村名から始まらない文字列は番地のリスト
                # 複数の番地を '、' で展開する。
                for subchiban in substr.split("、"):
                    chiban_list.append(
                        aza + subchiban.translate(self.trans_kansuji))

        return chiban_list

    def retrieve_fude(
        self,
        chiban: str,
        area: List[str],
        exact_match_only: bool = False
    ) -> dict:
        """
        地番表記住所をジオコーダで検索し、該当する筆を返します。

        Parameters
        ----------
        chiban: str
            検索する地番表記住所。
        area: List[str]
            対象エリア。都道府県名、市区町村名のリスト。
        exact_match_only: bool
            完全一致する筆だけを検索するか否か。

        Returns
        -------
        dict
            筆コードをキー、対応する住所ノードとステータスコードを
            値とする辞書。
        """
        @lru_cache(maxsize=1024)
        def get_fudecode(node):
            for note in node.note.split('/'):
                if note.startswith('fude:'):
                    return note[5:]

            return None

        fude_list = {}
        m = re.match(r'(.+)/(.+)/(.+)', chiban)
        if m is None:
            self.tree.set_config(target_area=area)
        else:
            self.tree.set_config(target_area=[m.group(1), m.group(2)])
            chiban = m.group(3)

        results = self.tree.searchNode(chiban)
        if len(results) == 0:
            return {"": (None, -1,)}

        node = results[0].node
        exact_match = len(results[0].matched) == len(chiban)
        code = get_fudecode(node)
        if code:
            # 一致した地番にコードが与えられている場合、
            # そのコードを持つ筆を利用する。
            # ステータスコードは 0。
            if exact_match_only and not exact_match:
                # 枝番で検索して地番がマッチした場合など
                fude_list[""] = (node, -1,)
            else:
                fude_list[code] = (node, 0,)

        elif node.level < 7:
            # 地番レベルまで住所を解析できなかった場合は
            # コードを付与できない。
            # ステータスコードは -1。
            fude_list[""] = (node, -1,)
        elif exact_match:
            # 地番レベルで検索して地番レベルのノードが見つかり、
            # かつその地番にコードが与えられていない場合は
            # コードが付与されている枝番を探す。
            # ステータスコードは 1。
            for child in node.children:
                code = get_fudecode(child)
                if code:
                    fude_list[code] = (child, 1,)
        else:
            # 枝番で検索して地番レベルのノードが見つかり
            # (＝枝番レベルで一致する筆は見つからなかった)
            # かつその地番にコードが与えられていない場合も
            # コードが付与されている枝番を探す。
            # ステータスコードは 2。

            # 仕様変更：ほぼ間違いなのでこの処理は行わない。
            # for child in node.children:
            #     code = get_fudecode(child)
            #     if code:
            #         fude_list[code] = (child, 2,)
            pass

        return fude_list

    @classmethod
    def decode_utf16(cls, matchobj):
        """
        UTF-16 コードをデコードします。

        Parameter
        ----------
        matchobj: re.match object
            group(0) に '<[0-9a-fA-F]+>' と一致する文字列
                （例: '<00008328>'）
            group(1) に '[0-9a-fA-F]+' と一致する文字列
                （例: '00008328'）

        Return
        ------
        str
            UTF-16 としてデコードした文字列（例: '茨'）
        """
        intval = int(matchobj.group(1), 16)
        char = intval.to_bytes(2, byteorder="big").decode('utf_16_be')
        return char

    @classmethod
    def repair_gaiji(cls, text: str) -> str:
        """
        正しく変換できていない外字を個別に修正します。

        Parameter
        ---------
        text: str
            外字を含む文字列。

        Return
        ------
        str
            修正した文字列。
        """
        for pair in (("\uee3e", "縢"),):
            text = text.replace(pair[0], pair[1])

        return text


class BuildingRegistryCleaner(ChibanUtil):
    """
    建物不動産登記データをクリーニングするクラス。
    """

    def clean_touki_building(
        self,
        code_prefix: str,
        fout: Optional[TextIOBase] = None
    ) -> int:
        """
        不動産登記（建物マスター）の所在および地番を参照し、
        ジオコーダを使って対応する筆ポリゴンのコードを付与します。
        結果は CSV 形式で fout に出力します。

        Paramters
        ---------
        code_prefix: str
            対象とする都道府県または市区町村のコード。
            都道府県の場合は２桁、市区町村の場合は５桁。
        fout: io.TextIOBase, optional
            出力先のストリーム。 None の場合は sys.stdout。

        Returns
        -------
        int
            出力した行数。
        """
        fout = fout or sys.stdout
        self.tree.set_config(aza_skip="on")  # ABR に記載がない字も省略を許可する

        nrecords = 0
        for row in dbman.select_records(
            "SELECT count(*) FROM building_master WHERE shikuchoson_code LIKE %s",
            (code_prefix + '%',)
        ):
            nrecords = row["count"]

        if nrecords == 0:
            return 0

        code_list = {}
        n = 0
        pbar = tqdm.tqdm(
            desc=f"Code {code_prefix}",
            total=nrecords,
            mininterval=0.2,
            ascii=False
        )
        sql = "SELECT * FROM building_master WHERE shikuchoson_code LIKE %s"
        values = (code_prefix + '%', )
        for row in dbman.select_records(sql, values):
            citycode = row["shikuchoson_code"]
            if citycode not in code_list:
                r = self.get_pref_city(citycode)
                if len(r) == 0:
                    shozai = row["shozai_oyobi_chiban"]
                    logger.warning((
                        f"'{citycode}' ({shozai}) は住所データがジオコーダに"
                        "登録されていないため、スキップします。"
                    ))
                    continue

                code_list[citycode] = r

            pbar.update(1)
            master_names = code_list[citycode]
            names = master_names[:]
            area = [x[1] for x in names]

            chiban = row["shozai_oyobi_chiban"]
            try:
                chiban = self.re_utf16.sub(self.__class__.decode_utf16, chiban)
                chiban = self.repair_gaiji(chiban)
                chiban_list = self.analyze_chiban(chiban, names)
            except (RuntimeError, OverflowError) as exc:
                logger.warning(f"{row['bldg_id']} :{exc}")
                continue

            # 筆コードを検索
            for i, chiban_fude in enumerate(chiban_list):
                j = 0
                retrieved = self.retrieve_fude(chiban_fude, area)
                for code, value in retrieved.items():
                    node, status = value
                    address = "".join(node.get_fullname())
                    level = node.level
                    citycode = node.get_city_jiscode()

                    pos = chiban_fude.rfind('/')
                    if pos >= 0:
                        chiban_fude = chiban_fude[pos + 1:]

                    line = self.get_line([
                        row["bldg_id"],
                        i,
                        chiban_fude,
                        j,
                        address,
                        citycode,
                        code,
                        "{:.6f}".format(node.x),
                        "{:.6f}".format(node.y),
                        level,
                        status,
                    ])
                    print(line, file=fout)
                    n += 1
                    j += 1

        pbar.close()
        return n


class LandRegistryCleaner(ChibanUtil):
    """
    土地不動産登記データをクリーニングするクラス。
    """

    def clean_touki_land(
        self,
        code_prefix: str,
        fout: Optional[TextIOBase] = None,
    ) -> int:
        """
        不動産登記（土地マスター）の所在および地番でグループ化した
        土地番号テーブルに対し、ジオコーダを使って対応する筆ポリゴンの
        コードを付与します。
        結果は CSV 形式で fout に出力します。

        Paramters
        ---------
        code_prefix: str
            対象とする都道府県または市区町村のコード。
            都道府県の場合は2桁、市区町村の場合は5桁。
        fout: io.TextIOBase, optional
            出力先のストリーム。 None の場合は sys.stdout。

        Returns
        -------
        int
            出力した行数。
        """
        fout = fout or sys.stdout
        self.tree.set_config(aza_skip="auto")  # ABR で起番フラグ=1 の字は省略可

        if len(code_prefix) >= 2:
            area = [self.prefcodes[code_prefix[0:2]]]
        else:
            area = []

        for row in dbman.select_records(
            "SELECT count(*) FROM tochi_bango WHERE 市区町村コード LIKE %s",
            (code_prefix + '%', )
        ):
            nrecords = row["count"]

        if nrecords == 0:
            return 0

        n = 0
        pbar = tqdm.tqdm(
            desc=f"Code {code_prefix}",
            total=nrecords,
            mininterval=0.2,
            ascii=False
        )
        for i, row in enumerate(dbman.select_records(
            "SELECT * FROM tochi_bango WHERE 市区町村コード LIKE %s",
            (code_prefix + '%', )
        )):
            pbar.update(1)
            chiban = row["所在"] + row["地番"]
            chiban = self.re_utf16.sub(self.__class__.decode_utf16, chiban)
            chiban = self.repair_gaiji(chiban)

            # 筆コードを検索
            # print(f"処理中: {i: 7d}:{chiban}")
            for code, res in self.retrieve_fude(
                    chiban, area, exact_match_only=True).items():
                _, status = res
                if status != 0:
                    continue

                line = self.get_line([
                    row["市区町村コード"],
                    row["所在"],
                    row["地番"],
                    row["登録の日"] or '',
                    row["土地id"],
                    code,
                ])
                print(line, file=fout)
                # print(f"  -> {n: 7d}:{code}")
                n += 1

        pbar.close()
        return n


class GeocodingDictionaryCreator(ChibanUtil):
    """
    14条地図の筆ポリゴンから、筆コード付きの住所ジオコーダ用
    辞書ファイル（テキスト形式）を生成するクラス。
    """

    re_chiban = re.compile(r'(^\d+)(\-\d.*$)?')

    @classmethod
    def get_dictionary_line(
        cls,
        names: List[Tuple[int, str]],
        x: float,
        y: float,
        note: str
    ) -> str:
        """
        住所ジオコーダ辞書ファイル（テキスト形式）の1行分の情報を作成し、
        文字列として返します。

        Parameters
        ----------
        names: [[int, str]]
            住所要素のレベル（整数）と名称の組のリスト。
        x: float
            X 値 (経度)。
        y: float
            Y 値 (緯度)。
        note: str, optional
            補足メモ。

        Notes
        -----
        - このクラスで生成する住所要素は、辞書の priority = 20 固定です。
        """
        line = ""
        for name in names:
            if name[1] != '':
                line += '{:s};{:d},'.format(name[1], name[0])

        line += '!{:02d},'.format(20)  # 辞書の priority
        line += "{:.6f},{:.6f}".format(x or 999.9, y or 999.9)
        if note is not None:
            line += ',{}'.format(str(note))

        return line

    def create_dicfile(
        self,
        prefcode: str,
        outf: TextIOBase
    ) -> int:
        """
        1都道府県分の住所ジオコーダ辞書（テキスト形式）を作成します。

        Parameters
        ----------
        prefcode: str
            対象の都道府県コード（2桁）。
        outf: TextIOBase
            出力先ストリーム（このクラスでは省略不可）。
        """
        code_list = {}
        prefpattern = "{:02d}%".format(prefcode)

        for row in dbman.select_records(
            "SELECT count(cd) FROM fude_master WHERE citycode LIKE %s",
            (prefpattern, )
        ):
            nrecords = row["count"]

        if nrecords == 0:
            return 0

        pbar = tqdm.tqdm(
            desc=f"Code {prefcode}",
            total=nrecords,
            mininterval=0.2,
            ascii=False
        )
        sql = (
            "SELECT cd, citycode, city, oaza, chome, aza, chiban, "
            "ST_X(center) AS lon, ST_Y(center) AS lat "
            "FROM fude_master WHERE cd LIKE %s"
        )
        values = (prefpattern, )
        n = 0
        for row in dbman.select_records(sql, values):
            pbar.update(1)
            citycode = row["citycode"]
            if citycode not in code_list:
                r = self.get_pref_city(citycode)
                if len(r) == 0:
                    city = row["city"]
                    raise RuntimeError(
                        f"'{citycode}' ({city}) はジオコーダに登録されていません。"
                    )

                code_list[citycode] = r

            master_names = code_list[citycode]
            names = master_names[:]
            if row["oaza"]:
                names.append([5, row["oaza"]])

            if row["chome"]:
                names.append([6, row["chome"]])

            if row["aza"]:
                names.append([6, row["aza"]])

            match = self.__class__.re_chiban.match(row["chiban"])
            if match:
                names.append([7, match.group(1) + '番地'])
                if match.group(2):
                    names.append([8, match.group(2)[1:]])

            print(
                self.get_dictionary_line(
                    names, row["lon"], row["lat"],
                    "fude:{}".format(row["cd"])
                ), file=outf)
            n += 1

        pbar.close()
        return n


class GappitsuGeocodingDictionaryCreator(ChibanUtil):
    """
    土地登記データの変更履歴から、14条地図に含まれている地番に
    合筆されている過去に存在した地番を追加するための
    筆コード付きの住所ジオコーダ用拡張辞書ファイル（テキスト形式）を
    生成するクラス。
    """

    re_gappitsu = re.compile(r'(.*分筆)?(.*)を合筆')
    re_chiban = re.compile(r'(同|本|\d+)番(\d*)(ないし(同|本|\d+)番(\d*))?')

    def __init__(self, jageocoder_db_dir: Optional[Path] = None) -> None:
        super().__init__(jageocoder_db_dir)
        self.tree.set_config(aza_skip="no")

    @classmethod
    def _generate_chiban_tuple(
        cls,
        chiban: str,
        edaban: str,
        base_chiban: str,
        cur_chiban: str
    ) -> Tuple[str, str]:
        """
        土地登記の変更履歴に含まれる地番表記を正規化します。

        Parameters
        ----------
        chiban: str
            土地登記の変更履歴に出現した地番表記文字列。
        edaban: str
            枝番。
        base_chiban: str
            処理中の登記の地番（ただし枝番を除く部分）。
        cur_chiban: str
            直前に出現した地番（ただし枝番を除く部分）。

        Returns
        -------
        (str, str)
            解釈した地番文字列と枝番文字列のセット。

        Notes
        -----
        - 「本番」は処理中の登記の地番に置き換えます。
        - 「同番」は直前の地番に置き換えます。
        """
        if chiban == '本':
            chiban = base_chiban
        elif chiban == '同':
            chiban = cur_chiban or base_chiban

        chiban = f'{chiban}番地'

        if edaban == '':
            return (chiban, )

        return (chiban, str(edaban))

    def create_gappitsu(
        self,
        code_prefix: str,
        fout: Optional[TextIOBase] = None
    ) -> int:
        """
        不動産登記（土地）から合筆情報を抽出し、合筆前の地番を検索すると
        合筆後の筆コードを返すジオコーダ用辞書ファイル（テキスト形式）を出力します。

        Parameters
        ----------
        code_prefix: str
            対象とする都道府県または市区町村のコード。
            都道府県の場合は2桁、市区町村の場合は5桁。
        fout: io.TextIOBase, optional
            出力先のストリーム。None の場合は sys.stdout。

        Returns
        -------
        int
            出力した行数。
        """
        fout = fout or sys.stdout

        for row in dbman.select_records((
            "SELECT count(*) FROM tochi_original "
            "WHERE 市区町村コード LIKE %s AND 変更履歴 IS NOT NULL"
        ), (code_prefix + '%', )):
            nrecords = row["count"]

        if nrecords == 0:
            return 0

        n = 0
        pbar = tqdm.tqdm(
            desc=f"Code {code_prefix}",
            total=nrecords,
            mininterval=0.2,
            ascii=False
        )
        for row in dbman.select_records((
            "SELECT * FROM tochi_original "
            "WHERE 市区町村コード LIKE %s AND 変更履歴 IS NOT NULL "
            "ORDER BY 登録の日 DESC"
        ), (code_prefix + '%', )):
            pbar.update(1)
            m = self.re_gappitsu.match(row["変更履歴"])
            if m is None:
                continue

            base_chiban = row["地番"]
            if '-' not in base_chiban:
                base_chiban_honban = base_chiban
            else:
                base_chiban_honban = base_chiban[0: base_chiban.rindex('-')]

            # 変更履歴の地番表記から正規化した地番リストを抽出
            matches = self.re_chiban.findall(m.group(2))
            cur_chiban = None
            chiban_list = []
            for match in matches:
                if match[2] != '':  # 「ないし」を含む
                    if match[1] != '':  # 枝番の範囲指定
                        if match[4] == '' and match[3] != '':
                            # 「同番XXX」の記載誤り
                            try:
                                for edaban in range(int(match[1]), int(match[3]) + 1):
                                    chiban_list.append(
                                        self.__class__._generate_chiban_tuple(
                                            match[0], edaban,
                                            base_chiban_honban, cur_chiban
                                        ))
                            except ValueError:
                                logger.warning(
                                    f"解析不能：'{match[0]}番{match[1]}{match[2]}'"
                                )
                                continue

                        else:
                            for edaban in range(int(match[1]), int(match[4]) + 1):
                                chiban_list.append(
                                    self.__class__._generate_chiban_tuple(
                                        match[0], edaban,
                                        base_chiban_honban, cur_chiban
                                    ))
                    else:  # 地番の範囲指定
                        try:
                            for ban in range(int(match[0]), int(match[3]) + 1):
                                chiban_list.append(
                                    self.__class__._generate_chiban_tuple(
                                        ban, '',
                                        base_chiban_honban, cur_chiban
                                    ))
                        except ValueError:
                            logger.warning(
                                f"解析不能：'{match[0]}番{match[1]}{match[2]}'"
                            )
                            continue

                else:
                    chiban_list.append(
                        self.__class__._generate_chiban_tuple(
                            match[0], match[1],
                            base_chiban_honban, cur_chiban
                        ))

                if match[0][0] in "0123456789":
                    cur_chiban = match[0]

            chiban = row["所在"] + (row["表示履歴地番"] or row["地番"])
            area = [row["市区町村コード"], ]
            for _, res in self.retrieve_fude(
                    chiban, area, exact_match_only=True).items():
                # 合筆後の住所ノードから合筆前の情報を生成
                node, status = res
                if status != 0:
                    # 合筆後の地番が登録されていない
                    continue

                address_elements = []
                cur = node
                while cur is not None:
                    if cur.level <= AddressLevel.AZA:
                        address_elements.insert(0, (cur.name, cur.level))

                    cur = cur.parent

                attribs = [
                    "!99",
                    "{:.6f}".format(node.x),
                    "{:.6f}".format(node.y),
                    node.note
                ]
                if "fude" not in node.note:
                    print(row)

                for record in chiban_list:
                    alias_elements = address_elements.copy()
                    if len(record) == 2:
                        alias_elements.append((record[0], AddressLevel.BLOCK))
                        alias_elements.append((record[1], AddressLevel.BLD))
                    else:
                        alias_elements.append((record[0], AddressLevel.BLOCK))

                    # 生成した合筆前の住所が、より最近起こった分筆処理で
                    # 上書きされていることがあるので、
                    # 検索できるかどうかで簡易的にチェックする。
                    skip_record = False
                    cchiban = row["所在"] + ''.join(record)
                    for _, cres in self.retrieve_fude(
                            cchiban,
                            area, exact_match_only=True).items():
                        cstatus = cres[1]
                        if cstatus == 0:
                            # この住所は登録済みなので除外する
                            skip_record = True
                            break
                        else:
                            skip_record = False
                            break

                    if skip_record:
                        continue

                    nodes = []
                    for e in alias_elements:
                        nodes.append(f"{e[0]};{e[1]}")

                    nodes += attribs
                    line = self.get_line(nodes)
                    print(line, file=fout)
                    n += 1

        pbar.close()
        return n
