# サービス環境実行方法

## 概要

建物のCityGMLを入力とし、不動産IDを付与したCityGMLを出力するスクリプトです。

## ディレクトリ構成

本スクリプトを実行するためのディレクトリ構成は以下の通りです。

/estate_id_batch
    /batch
        /data
            /input
            /output
        /src
            main.py
        Dockerfile

## 前提条件

- 本スクリプトは、Dockerコンテナ、および、AWS Batch環境で実行することを想定しています。予め利用OSにDocker環境をインストールしておいてください。
- 処理可能なのは建物のCityGMLのみです。そのほかの種類のCityGMLは処理できません。

## 実行手順

本スクリプトをWindows PowerShellで実行する場合は、以下の手順で実行してください。

1. Dockerfileファイルが置かれているディレクトリに移動します。
```
PS C:\estate_id_batch\batch> cd estate_id_batch/batch
```

2. Dockerfileファイルからdocker buildを行い、ローカルマシンにイメージを作成します。
```
PS C:\estate_id_batch\batch> docker build -t estate_matching_image .
```

出力結果例:
```
 => [internal] load build definition from Dockerfile                                                                           0.0s
 => => transferring dockerfile: 32B                                                                                            0.0s
 => [internal] load .dockerignore                                                                                              0.0s
 => => transferring context: 2B                                                                                                0.0s
 => [internal] load metadata for ghcr.io/osgeo/gdal:ubuntu-small-3.7.1                                                         1.0s
 => [internal] load build context                                                                                              0.0s
 => => transferring context: 49.64kB                                                                                           0.0s
 => [1/6] FROM ghcr.io/osgeo/gdal:ubuntu-small-3.7.1@sha256:25c22fd2d889a09362ec12b39ddf26530198a2943fbf8160c7b921ac86c50e6a   0.0s
 => CACHED [2/6] RUN apt-get update &&     apt-get install -y postgresql-client &&     apt-get install -y python3-pip          0.0s
 => CACHED [3/6] RUN pip install boto3 psycopg2-binary install python-dotenv lxml requests pytz                                0.0s
 => CACHED [4/6] WORKDIR /app                                                                                                  0.0s
 => CACHED [5/6] RUN mkdir /app/data                                                                                           0.0s
 => [6/6] COPY src /app/src                                                                                                    0.0s
 => exporting to image                                                                                                         0.0s
 => => exporting layers                                                                                                        0.0s
 => => writing image sha256:4cca64457fea7f0e571a2ebccb4c3822a3aac7b636cd5430ba9fcf19b67f6464                                   0.0s
 => => naming to docker.io/library/estate_matching_image                                                                       0.0s
```

3. buildが完了したイメージを実行します。

次のコマンドを用いて、イメージ内で使っているOS（Linux）にログインします。

```
docker run -it -v ${pwd}:/app estate_matching_image bash
```

4. イメージ内のOSに環境変数を定義します。

```
root@0344a7d63e05:/app# export USE_ESTATE_ID_CONFIRMATION_SYSTEM=0
root@0344a7d63e05:/app# export NO_USE_IAM_MODE=1
root@0344a7d63e05:/app# export ESTATE_ID_USER_ID=[任意のユーザID文字列]
root@0344a7d63e05:/app# export ESTATE_ID_SESSION_ID=[任意のセッションID文字列]
```

5. CityGMLファイルをdata/input以下のディレクトリに配置します。

スクリプトで処理するCityGMLの配置ディレクトリにはルールがあり、

data/input/[ESTATE_ID_USER_ID]/[ESTATE_ID_SESSION_ID]というディレクトリから処理するCityGMLファイルを読み取ります。

data/input/[ESTATE_ID_USER_ID]/[ESTATE_ID_SESSION_ID]ディレクトリを作成し、その中にCityGMLファイルを配置してください。

例：

- ESTATE_ID_USER_ID: matsuyama
- ESTATE_ID_SESSION_ID: session_1
と設定している場合、

data/input/matsuyama/session_1というディレクトリを作成し、その中に処理をするCityGMLファイルを配置してください。

6. pythonスクリプトsrc/main.pyを実行します。

```
root@0344a7d63e05:/app# python src/main.py
```

実行時のログが画面に出力されます。

```
data/input/test-user/matsuyama_session_1
Download completed.
lod0RoofEdge
50324684_bldg_6697_op.gmlをインポート中...
50324684_bldg_6697_op.gmlをインポート完了
lod0RoofEdge
50324685_bldg_6697_op.gmlをインポート中...
50324685_bldg_6697_op.gmlをインポート完了
lod0RoofEdge
50324686_bldg_6697_op.gmlをインポート中...
50324686_bldg_6697_op.gmlをインポート完了
lod0RoofEdge
50324687_bldg_6697_op.gmlをインポート中...
50324687_bldg_6697_op.gmlをインポート完了
lod0RoofEdge
50324694_bldg_6697_op.gmlをインポート中...
50324694_bldg_6697_op.gmlをインポート完了
lod0RoofEdge
50324695_bldg_6697_op.gmlをインポート中...
50324695_bldg_6697_op.gmlをインポート完了
lod0RoofEdge

...（略）...

50325567_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ件数: 6件
50325568_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ件数: 237件
50325569_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ件数: 14件
50325574_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ追加件数: 0件
50325575_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ件数: 5件
50325576_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ追加件数: 0件
50325577_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ件数: 538件
50325578_bldg_6697_op.gmlにマッチング結果を付与マッチング処理...
uro_uri https://www.geospatial.jp/iur/uro/3.0
マッチングデータ件数: 2493件
delete building_citygml_matched, building_citygml table data.
desirialize
```

7. data/outputディレクトリにディレクトリが生成され、処理したCityGMLファイルが保存されていることを確認します。

## 諸注意

- 本スクリプトは、Dockerコンテナ、および、AWS Batch環境で実行することを想定しています。
- 一度に処理するファイル数は、50個程度に留めてください。
- DBはAIGID AWSのRDSを利用しています。特定のIPアドレスからのみアクセス可能です。

以上
