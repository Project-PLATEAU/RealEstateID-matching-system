"""
PostgreSQL データベースへのアクセスを管理するクラスライブラリ。
"""
import logging
import os
from typing import Iterator

import psycopg2
from psycopg2.extras import DictCursor

logger = logging.getLogger(__name__)


class DBManager(object):

    def __init__(self):
        self.dsn = self.__class__.get_dsn()

    @classmethod
    def get_dsn(cls) -> str:
        """
        環境変数から DSN 文字列を生成します。

        Notes
        -----
        - 以下の環境変数を参照します。カッコ内はデフォルト値。
        - PGHOST: ホスト名 ('localhost')
        - PGPORT: ポート番号 ('5432')
        - PGDB: データベース名 ('pgdb')
        - PGUSER: ロール名 ('pguser')
        - PGPASS: パスワード ('pgpass')
        """
        pghost = os.environ.get('PGHOST', 'localhost')
        pgport = os.environ.get('PGPORT', '5432')
        pgdb = os.environ.get('PGDB', 'pgdb')
        pguser = os.environ.get('PGUSER', 'pguser')
        pgpass = os.environ.get('PGPASS', 'pgpass')
        dsn = (
            f"host={pghost} "
            f"dbname={pgdb} "
            f"port={pgport} "
            f"user={pguser} "
            f"password={pgpass}"
        )
        return dsn

    def select_records(
        self, query: str, params: tuple
    ) -> Iterator[dict]:
        """
        データベースに接続して SELECT を実行するジェネレータ。
        検索結果は yield で返します。

        Parameters
        ----------
        query: str
            SELECT SQL 文。プレースホルダ '%s' を利用できます。
        params: tuple
            プレースホルダに渡す変数のリスト。
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, params)
                for row in cur:
                    yield row
