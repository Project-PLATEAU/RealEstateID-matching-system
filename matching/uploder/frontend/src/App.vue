<script setup lang="ts">
import Upload from './components/Upload.vue'
import { Authenticator, translations } from '@aws-amplify/ui-vue'
import { I18n } from 'aws-amplify'
import '@aws-amplify/ui-vue/styles.css'

I18n.putVocabularies(translations)
I18n.setLanguage('ja')
</script>

<template>
    <authenticator>
        <template v-slot:header>
            <div
                style="padding: var(--amplify-space-large)"
                class="text-center"
            >
                <h1 class="amplify-heading">不動産IDマッチングシステム</h1>
            </div>
        </template>
        <template v-slot:footer>
            <div
                style="padding: var(--amplify-space-large)"
                class="text-center"
            >
                <p
                    class="amplify-text"
                    style="color: var(--amplify-colors-neutral-80)"
                >
                    Powerd by AIGID
                </p>
            </div>
        </template>
        <template v-slot="{ signOut }">
            <div
                style="padding: var(--amplify-space-large)"
                class="text-right"
            >
                <button
                    @click="signOut"
                    class="relative cursor-pointer rounded-md bg-white underline underline-offset-4 font-semibold text-indigo-600 hover:text-indigo-500"
                >
                    ログアウト
                </button>
            </div>
            <Upload />
        </template>
    </authenticator>
</template>

<style scoped>
#app {
    font-family: Avenir, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #2c3e50;
    margin-top: 60px;
}
</style>
