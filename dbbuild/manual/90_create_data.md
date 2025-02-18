---
layout: page
title: 不動産ID空間データ作成方法
permalink: /create_realestate_id_data
---

このページでは、「不動産IDマッチングシステム」で利用する
「不動産ID空間データ」の作り方について説明します。

なお、データ作成に利用する「不動産登記データ」は非公開なので、
代わりに個人情報等を含まないダミーデータをサンプルとして収録しています。

## <a name="about_realestate_id_data">「不動産ID空間データ」とは</a>

不動産IDマッチングシステムは、 3次元都市モデルの建物データに含まれる建物形状および
高さ・建物構造などの各種情報と、不動産登記データに含まれる床面積や階数などの
登記情報を比較し、もっとも一致する不動産登記の「不動産番号」を元に
建物の不動産IDを計算して付与します。
また、建物データの座標と不動産登記データの所在地の座標が
一致しているかを確認するため、登記所備付地図を利用して
所在地に対応する座標の範囲（ポリゴン）を求めています。

この処理を高速に行うため、不動産登記データと登記所備付地図を
地理情報を管理できるリレーショナルデータベース (PostGIS) に
検索可能な形で登録した空間データベースを作成しておく必要があります。
このデータベースを「不動産ID空間データ」と呼んでいます。

これ以降で説明するのは、下図右側の「変換」に当たる処理を行い、
入力データから不動産ID空間データを作成する手順です。

<img src="./realestate_id_data.png" title="不動産ID空間データ" style="border: 1px solid #888;" />

## <a name="required_datasets">必要なデータセット</a>

不動産ID空間データを構築するには、対象とする地域の以下のデータが必要です。

- 不動産登記（建物）データ
- 不動産登記（土地）データ
- 登記所備付地図データ

不動産登記（建物・土地）データは CSV 形式、登記所備付地図データは
GeoJSON 形式のものを用意してください。

> 不動産登記（建物）データには、建物の不動産番号のほか、
> 建物の面積や階数、用途種別などマッチングに最も必要な情報が含まれています。
>
> 不動産登記（土地）データは上の図では省略していますが、
> 土地の分筆や合筆の履歴、および土地の不動産番号が含まれています。
> 建物の新築時の登記に記載されている所在地は登記時点のものなので、
> その後で他の土地と合筆されていると現存しないことがあります。
> 登記時点の所在地記載を現在の所在地に変換するため、土地の履歴を
> 利用する必要があります。
> また、分譲マンションなどの区分所有建物の不動産IDは「土地の不動産番号」から
> 計算すると決められているため、そこでも土地の登記データを利用します。
> 
> 最後の登記所備付地図データは、不動産登記（建物）データと
> 不動産登記（土地）データに含まれる所在地に対応する座標を計算するために
> 利用します。

## <a name="create_data">不動産ID空間データ作成手順</a>

- Docker 環境構築

    データ作成には PostGIS や GDAL, Python などが必要ですが、
    Docker を利用すると環境構築を自動的に行うことができます。
    使用する OS に合わせて <a href="https://docs.docker.com/get-docker/">
    Docker Desktop または Docker Engine をインストール</a>してください。

- PostGIS 起動

    不動産ID空間データは PostGIS 上に構築されますので、まず PostGIS を
    バックグランド（ディタッチドモード）で起動してください。

        $ docker compose up -d postgis_realestate

- 入力データファイルを配置

    input ディレクトリに入力データファイルを配置します。

    - 不動産登記（建物）データのファイル名は必ず `*_tatemono*.csv.gz` 
      にマッチする必要があります（*はワイルドカード）。
      ファイルフォーマットは CSV を gzip で圧縮してください。

    - 不動産登記（土地）データのファイル名は必ず `*_tochi*.csv.gz` 
      にマッチする必要があります（*はワイルドカード）。
      ファイルフォーマットは CSV を gzip で圧縮してください。

    - 登記所備付地図データのファイル名は必ず `*.geojson.gz`
      にマッチする必要があります（*はワイルドカード）。
      ファイルフォーマットは GeoJSON を gzip で圧縮してください。

    サンプルデータを配置する場合は、 sample の下にある
    37201_dummy_tatemono.csv.gz, 37201_dummy_tochi.csv.gz,
    37201_sample.geojson.gz の3ファイルを input/ にコピーしてください。
    
- データ作成スクリプトを実行

    realestate_id_db サービスを実行すると作成処理を実行します。
    
        $ docker compose run --rm realestate_id_db
    
    正常に処理が完了すると `work/realestate_id_db-YYYYMMDD_hhmmss.dump.gz`
    に不動産ID空間データをすべて含む PostGIS ダンプファイルが生成されます。
    ただし YYYYMMDD は処理を開始した年月日、 hhmmss は時分秒です。

    このダンプファイルをマッチングシステムの PostGIS に読み込めば
    3次元都市モデルの建物ポリゴンとマッチングを行うことができます。

- 作業用 Docker コンテナ・イメージの削除

    作業が終わり、この後データ作成用の Docker コンテナを利用しない場合は、
    以下のコマンドで作成したコンテナを削除します。

        $ docker compose down -v

    また、イメージも不要であれば削除して構いません。

        $ docker rmi postgis_realestate
        $ docker rmi realestate_id_db

不動産ID空間データを作成する手順は以上です。
