import { createApp } from 'vue'
import App from './App.vue'
import './main.css';
import { Amplify } from 'aws-amplify';

Amplify.configure({
    // Amazon Cognito認証用の初期設定
    Auth: {
        region: import.meta.env.VITE_COGNITO_REGION,
        userPoolId: import.meta.env.VITE_COGNITO_USER_POOL_ID,
        userPoolWebClientId: import.meta.env.VITE_COGNITO_CLIENT_ID,
        identityPoolId: import.meta.env.VITE_COGNITO_IDENTITY_POOL_ID
    }
});

createApp(App).mount('#app')
