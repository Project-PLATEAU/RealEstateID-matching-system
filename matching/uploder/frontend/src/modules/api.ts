import { SignatureV4 } from '@aws-sdk/signature-v4'
import { Sha256 } from '@aws-crypto/sha256-js'
import { HttpRequest } from '@aws-sdk/protocol-http'
import { Amplify } from 'aws-amplify'

const apiHostName = import.meta.env.VITE_API_ENDPOINT as string

const fetchWithSignature = async (
    method: 'GET' | 'POST' = 'POST',
    path: string,
    body: BodyInit | undefined = undefined
) => {
    const credentials = await Amplify.Auth.currentCredentials()

    const signer = new SignatureV4({
        region: 'ap-northeast-1',
        service: 'lambda',
        sha256: Sha256,
        credentials,
    })

    const req = await signer.sign(
        new HttpRequest({
            method,
            protocol: 'https:',
            path,
            hostname: apiHostName,
            headers: {
                host: apiHostName,
                'Content-Type': 'application/json',
            },
            body: body,
        })
    )

    const res = await fetch(`https://${apiHostName}${path}`, {
        method: req.method,
        body: req.body,
        headers: req.headers,
    })

    return res
}

const getPresignedURL = async (session_id: string, fileName: string) => {
    const authUserData = await Amplify.Auth.currentSession()

    const res = await fetchWithSignature(
        'POST',
        '/upload_url',
        JSON.stringify({
            session_id,
            user_id: authUserData.idToken.payload.sub,
            object_name: fileName,
        })
    )

    const _json = (await res.json()) as { signed_url: string }
    return _json
}

/**
 * 事前に取得したS3のpresigned-urlにファイルをアップロードする
 */
const uploadToS3 = async (preSignedUrl: string, file: File) => {
    const res = await fetch(preSignedUrl, {
        method: 'PUT',
        headers: {
            'Content-Type': file.type,
        },
        body: file,
    })
    return res
}

/**
 * 複数ファイルのアップロードの完了をAPIに通知する
 */
const emitUploadComplete = async (session_id: string) => {
    const authUserData = await Amplify.Auth.currentSession()
    const res = await fetchWithSignature(
        'POST',
        '/receipt_request',
        JSON.stringify({
            session_id,
            user_id: authUserData.idToken.payload.sub,
        })
    )

    return res
}

export { getPresignedURL, emitUploadComplete, uploadToS3 }
