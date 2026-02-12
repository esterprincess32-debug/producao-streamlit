# Deploy no Render + Firebase

## 1) Suba o projeto no GitHub
No diretório do projeto:

```powershell
git init
git add .
git commit -m "app pronto para deploy no render"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/SEU_REPO.git
git push -u origin main
```

## 2) Crie o serviço no Render
1. Acesse `https://dashboard.render.com/`
2. `New` -> `Blueprint`
3. Conecte o repositório GitHub.
4. O Render vai ler `render.yaml` e criar o serviço.

## 3) Configure o segredo do Firebase
No Render, no serviço criado:
1. `Environment`
2. Adicione variável:
   - `FIREBASE_SERVICE_ACCOUNT_JSON`
   - valor: JSON completo da service account do Firebase (em uma linha)

`USE_FIREBASE=true` já está no `render.yaml`.

## 4) Deploy
1. Clique `Manual Deploy` (ou aguarde auto-deploy).
2. Ao finalizar, abra a URL pública gerada pelo Render.

## 5) Teste de login
Usuários iniciais (se ainda não alterou):
- `admin` / `admin123`
- `consulta` / `consulta123`

