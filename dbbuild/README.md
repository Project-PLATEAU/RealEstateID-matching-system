# 不動産ID空間データ作成

- 準備

    Docker Server または Docker Desktop をインストールします。

    このディレクトリで Docker イメージを作成します。

        $ docker compose build

    input/ ディレクトリに以下の３種類のファイルを配置します。

    - 登記所備付地図データ
    
        GeoJSON 形式のファイルを個別に gzip 圧縮したもの。
        ファイル名は DDDDD_\*.geojson.gz である必要があります。
        DDDDD の部分には市区町村コードを指定してください。

    - 建物登記データ

        CSV 形式のファイルを個別に gzip 圧縮したもの。
        ファイル名は \*tatemono\*.csv.gz である必要があります。

    - 土地登記データ

        CSV 形式のファイルを個別に gzip 圧縮したもの。
        ファイル名は \*tochi\*.csv.gz である必要があります。

    sample/ のサンプルデータを利用する場合はコピーしてください。

        $ cp sample/*.gz input/

- 実行

    まず PostGIS サーバをディタッチドモードで起動します。

        $ docker compose up -d postgis_realestate

    次に変換プログラムを実行します。

        $ docker compose run --rm realestate_id_db
    
    処理が完了すると、PostGIS を pg_dump でダンプしたファイルが
    `work/realestate_id_db-YYYYMMDD_hhmmss.dump.gz` に生成されます。
    YYYYMMDD_hhmm は実行開始時の年月日・時分秒です。

以上。
