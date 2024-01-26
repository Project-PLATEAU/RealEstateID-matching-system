import boto3
from fastapi import Depends, FastAPI
from typing_extensions import Annotated
from mangum import Mangum
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from functools import lru_cache
import config
import datetime

@lru_cache()
def get_settings():
    return config.Settings()

# 指定したユーザーのメールアドレスを取得する
def get_cognito_mail_address(user_id):
    settings = get_settings()

    # cognito user pool search
    cognito_client = boto3.client('cognito-idp')

    users = cognito_client.list_users(
        UserPoolId=settings.user_pool_id,
        Filter=f'sub = "{user_id}"',
    ).get('Users')

    # user not found error
    if len(users) == 0:
        return {"error": "user not found"}

    # print(users[0])
    email_address = ""
    for item in users[0]["Attributes"]:
        if item.get("Name") == "email":
            email_address =item.get("Value")

    return {"email_address": email_address}

# 指定したメールアドレスにメールを送信する
def send_email(to_email_address, subject, body):
    settings = get_settings()

    ses_client = boto3.client('ses', region_name='ap-northeast-1')
    source_mail_address = settings.ses_source_email_address

    response = ses_client.send_email(
        Source=source_mail_address,
        Destination={
            'ToAddresses': [
                to_email_address,
            ]
        },
        Message={
            'Subject': {
                'Data': subject,
            },
            'Body': {
                'Text': {
                    'Data': body,
                },
            }
        }
    )
    return response

# S3バケット/[input|output]/[user_id]/[session_id]/に格納されているファイル一覧を取得する
def list_buckets(target_obj, user_id, session_id):
    settings = get_settings()
    bucket = settings.signed_url_bucket

    if target_obj not in ("input", "output"):
        return {"error": "invalid target object"}

    prefix = f"data/{target_obj}/{user_id}/{session_id}/"

    s3 = boto3.resource('s3')

    s3_bucket_obj = s3.Bucket(bucket)
    objs = s3_bucket_obj.meta.client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
        )

    if ('Contents' not in objs):
        return {"error": "invalid target object"}
        # print('bucketName={0}, folderName={1}'.format(bucket, prefix))

    return objs['Contents']


# Define a Pydantic model for the generate upload url's request body
class UploadSessionInfo(BaseModel):
    session_id: str
    user_id: str
    object_name: str

# Define a Pydantic model for the generate upload url's request body
class RequestSessionInfo(BaseModel):
    session_id: str
    user_id: str

# FastAPI app
app = FastAPI()

# / endpoint
@app.get("/")
async def hello_world():
    return {"hello": "world aaaa bbbb"}

# /upload_url endpoint
@app.post("/upload_url")
async def get_upload_url(session_info: UploadSessionInfo):
    settings = get_settings()
    bucket = settings.signed_url_bucket
    expires_in = settings.signed_url_expires_in

    key = "data/input/{}/{}/{}".format(
        session_info.user_id, session_info.session_id, session_info.object_name
        )

    s3_client = boto3.client('s3')
    response = s3_client.generate_presigned_url(
        'put_object',
        Params={
            'Bucket': bucket,
            'Key': key
        },
        ExpiresIn=expires_in
        )

    return JSONResponse(content={
        "user_id": session_info.user_id,
        "session_id": session_info.session_id,
        "object_name": session_info.object_name,
        "signed_url": response
        })

# /receipt_request endpoint
@app.post("/receipt_request")
async def get_receipt_request(session_info: RequestSessionInfo):
    settings = get_settings()

    # S3バケット input/[ユーザID]/[セッションID]にアップロードしたgmlファイル一覧を取得
    gml_files_s3_obj = list_buckets(
        "input",
        session_info.user_id,
        session_info.session_id
        )
    gml_file_list = []
    for gml_file in gml_files_s3_obj:
        if gml_file.get("Key").endswith(".gml"):
            gml_file_name = gml_file.get("Key").split("/")[-1]
            gml_file_list.append(gml_file_name)

    # batch job submit
    response = {}
    batch_client = boto3.client('batch')

    response = batch_client.submit_job(
        jobName = settings.job_name,
        jobQueue = settings.job_queue,
        jobDefinition = settings.job_definition,
        containerOverrides = {
            'command': ["python","src/main.py"],
            'environment': [
                {"name": "ESTATE_ID_USER_ID", "value": session_info.user_id},
                {"name": "ESTATE_ID_SESSION_ID", "value": session_info.session_id}
            ]
        }
    )

    # get email address
    obj = get_cognito_mail_address(session_info.user_id)
    if obj.get("error") is not None:
        return JSONResponse(content=obj)

    to_email_address = obj.get("email_address")

    # send email to user
    mail_subject = "[不動産ID] 受付完了いたしました"

    mail_body  = f"ユーザID: {to_email_address} 様からご登録いただいた"
    mail_body += "CityGML ファイルについて、更新処理を開始いたしました。\n"
    mail_body += "\n"
    mail_body += "処理完了までしばらくお待ちください。"
    mail_body += "\n"
    mail_body += "[アップロードしたファイル一覧]\n\n"
    for gml_file in gml_file_list:
        mail_body += f"  - {gml_file}\n"

    response = send_email(
        to_email_address,
        mail_subject,
        mail_body
        )

    return JSONResponse(content={
        "response": response
    })

# /job_complete endpoint
@app.post("/job_complete")
async def get_job_complete(session_info: RequestSessionInfo):

    # 指定したS3バケット output/ユーザID/セッションIDに格納されているZIPファイル一覧を取得する
    zip_files_s3_obj = list_buckets(
        "output",
        session_info.user_id,
        session_info.session_id
        )
    # ZIPファイル一覧が得られずエラーの場合はエラーを返す
    if 'error' in zip_files_s3_obj:
        if zip_files_s3_obj.get("error") is not None:
            return JSONResponse(content={
                "error": "Target user_id or session_id is not found"
                })

    # 指定したS3バケット output/ユーザID/セッションIDに、ZIPファイルが存在するか確認する
    zip_file_key = ""
    for zip_file in zip_files_s3_obj:
        if zip_file.get("Key").endswith(".zip"):
            zip_file_key = zip_file.get("Key")
            break

    # ZIPファイルが存在しない場合はエラーを返す
    if zip_file_key == "":
        return JSONResponse(content={
            "error": "zip file not found"
            })

    # cognito からユーザIDを元にメールアドレスを取得する
    obj = get_cognito_mail_address(session_info.user_id)
    if obj.get("error") is not None:
        return JSONResponse(
            content=obj
            )

    to_email_address = obj.get("email_address")

    # send email to user
    return JSONResponse(content={
        "user_id": session_info.user_id,
        "session_id": session_info.session_id,
        "email": to_email_address
        })

# entry point for lambda function.
lambda_handler = Mangum(app)
