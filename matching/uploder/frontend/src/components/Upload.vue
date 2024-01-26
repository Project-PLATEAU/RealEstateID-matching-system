<script setup lang="ts">
import { PhotoIcon } from '@heroicons/vue/24/solid'
import { ref, computed } from 'vue'

import * as api from '../modules/api'

type UploadFile = {
    file: File
    status:
        | '待機中'
        | 'URL取得中'
        | 'URL取得失敗'
        | 'アップロード中'
        | 'アップロード失敗'
        | 'アップロード完了'
        | 'サイズ上限超過'
        | '対象外ファイル'
}

/**
 * UIで選択されたファイル一覧＋処理ステータス
 */
const uploadFileObjects = ref<Array<UploadFile>>([])
const isLoading = ref(false) // API通信中か否か

const fileSizeLimit = 1024 * 1024 * 1024 // 1GB

/**
 * アップロード処理を実行できるかどうかを表す
 * アップロードが実行可能な条件＝API通信中、ファイルが1つ以上選択されている、選択されたファイルが全てサイズ制限以内
 */
const isUploadExcutable = computed(() => !isLoading.value &&
        uploadFileObjects.value.length > 0 &&
        uploadFileObjects.value.every(
            (uploadFile) => uploadFile.file.size <= fileSizeLimit
        )
)

const loadSelectedFiles = (files: File[]) => {
    uploadFileObjects.value = files.map((file) => {
        const fileObject: UploadFile = {
            file,
            status: '待機中',
        }
        if (file.size > fileSizeLimit) fileObject.status = 'サイズ上限超過'
        //.gmlファイル以外の場合
        if (file.name.slice(-4) !== '.gml') fileObject.status = '対象外ファイル'
        return fileObject
    })

    if (
        uploadFileObjects.value.some(
            (uploadFile) => uploadFile.status === 'サイズ上限超過'
        )
    ) {
        alert('ファイルサイズが1GBを超えています。')
    }

    if (
        uploadFileObjects.value.some(
            (uploadFile) => uploadFile.status === '対象外ファイル'
        )
    ) {
        alert('gmlファイル以外が選択されています。')
    }
}

const onStockUploadFile = (e: any) => {
    loadSelectedFiles(Array.from(e.target.files))
}

const onDropUploadFile = (e: any) => {
    e.preventDefault()
    e.target.classList.remove('border-indigo-600')
    loadSelectedFiles(Array.from(e.dataTransfer.files))
}

const onDragover = (e: any) => {
    e.preventDefault()
    e.target.classList.add('border-indigo-600')
}

const onDragleave = (e: any) => {
    e.preventDefault()
    e.target.classList.remove('border-indigo-600')
}

const onUploadFile = async (e: any) => {
    e.preventDefault()

    // アップロード中はボタンを無効化する
    isLoading.value = true

    // セッションIDを生成：任意のUUID
    const session_id = crypto.randomUUID()

    // アップロードファイル数カウント
    let uploadFileCount = 0

    // ファイルアップロード処理を実装する
    // ここでLambdaに対して署名付きURLを取得する
    // 1ファイルごとに行う
    for (let i = 0; i < uploadFileObjects.value.length; i++) {
        // サイズ上限超過、対象外ファイルはスキップ
        if (
            uploadFileObjects.value[i].status === 'サイズ上限超過' ||
            uploadFileObjects.value[i].status === '対象外ファイル'
        ) {
            continue
        }
        uploadFileObjects.value[i].status = 'URL取得中'

        let result
        try {
            result = await api.getPresignedURL(
                session_id,
                uploadFileObjects.value[i].file.name
            )
        } catch {
            uploadFileObjects.value[i].status = 'URL取得失敗'
            alert('エラー：アップロードURLが取得出来ませんでした')
            isLoading.value = false
            return
        }

        const presignedUrl = result.signed_url

        uploadFileObjects.value[i].status = 'アップロード中'
        const uploadResult = await api.uploadToS3(
            presignedUrl,
            uploadFileObjects.value[i].file
        )

        if (uploadResult.status !== 200) {
            console.log(uploadResult)
            uploadFileObjects.value[i].status = 'アップロード失敗'
            alert('アップロードに失敗しました')
            isLoading.value = false
            return
        }

        uploadFileObjects.value[i].status = 'アップロード完了'
        uploadFileCount++
    }

    // 1件も無かったら抜ける
    if (uploadFileCount === 0) {
        alert('アップロード対象ファイルがありません')
        isLoading.value = false
        return
    }

    const emitResult = await api.emitUploadComplete(session_id)
    if (emitResult.status !== 200) {
        console.log(emitResult)
        alert('アップロード完了通知に失敗しました')
        isLoading.value = false
        return
    }

    alert('アップロードが完了しました')

    isLoading.value = false
}
</script>

<template>
    <header class="mt-10">
        <div class="container mx-auto max-w-xl">
            <div class="md:flex md:items-center md:justify-between">
                <div class="min-w-0 flex-1">
                    <h2
                        class="text-xl font-bold leading-7 text-gray-900 sm:truncate sm:text-2xl sm:tracking-tight"
                    >
                        GMLファイルアップロード
                    </h2>
                </div>
            </div>
        </div>
    </header>
    <main class="mt-5">
        <div class="container mx-auto max-w-xl">
            <form>
                <div class="space-y-12">
                    <div class="col-span-full">
                        <div class="flex justify-between back p-2 bg-slate-300 text-xs">
                                GMLファイルを選択（複数ファイル同時選択可能）すると、Uploadボタンが活性化するので、ボタンを押下します。するとデータのアップロードが開始されます。<br/>
                                アップロードの最中はブラウザ移動せず、そのままにしてください。データ破損の原因となります。<br/>
                                選択するファイルを間違えた場合は、新たにファイル選択をすることでアップロードの対象ファイルが上書きされます。
                        </div>
                        <div class="flex justify-between mt-5">
                            <h3
                                class="text-lg leading-6 font-medium text-gray-900"
                            >
                                ファイルを選択
                            </h3>
                        </div>
                        <div
                            class="mt-2 flex justify-center rounded-lg border border-dashed border-gray-900/25 px-6 py-10"
                            @dragover="onDragover"
                            @dragleave="onDragleave"
                            @drop="onDropUploadFile"
                        >
                            <div class="text-center">
                                <PhotoIcon
                                    class="mx-auto h-12 w-12 text-gray-300"
                                    aria-hidden="true"
                                />
                                <div
                                    class="mt-4 flex text-sm leading-6 text-gray-600"
                                >
                                    <label
                                        for="file-upload"
                                        class="relative cursor-pointer rounded-md bg-white font-semibold text-indigo-600 focus-within:outline-none focus-within:ring-2 focus-within:ring-indigo-600 focus-within:ring-offset-2 hover:text-indigo-500"
                                    >
                                        <span>ファイルを選択する</span>
                                        <input
                                            id="file-upload"
                                            name="file-upload"
                                            type="file"
                                            class="sr-only"
                                            accept=".gml"
                                            @change="onStockUploadFile"
                                            multiple
                                        />
                                    </label>
                                    <p class="pl-1">または、ドラッグアンドドロップする</p>
                                </div>
                                <p class="text-xs leading-5 text-gray-600">
                                    CityGML(.gml) to 1GB
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="mt-5">
                    <ul>
                        <li
                            class="flex flex-nowrap"
                            v-for="obj in uploadFileObjects"
                        >
                            <p class="text-sm">{{ obj.file.name }} 【{{ obj.status }}】</p>
                        </li>
                    </ul>
                </div>
                <div class="mt-6 flex items-center justify-end gap-x-6">
                    <button
                        type="submit"
                        @click="onUploadFile"
                        :disabled="!isUploadExcutable"
                        :class="{
                            'opacity-50 cursor-not-allowed': !isUploadExcutable,
                        }"
                        class="rounded-md bg-indigo-600 px-3 py-2 text-sm font-semibold text-white shadow-sm enabled:hover:bg-indigo-500 enabled:focus-visible:outline enabled:focus-visible:outline-2 enabled:focus-visible:outline-offset-2 enabled:focus-visible:outline-indigo-600"
                    >
                        Upload
                    </button>
                </div>
            </form>
        </div>
    </main>
</template>

<style scoped></style>
