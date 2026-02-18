# Deploy no Zeabur

## 1) Criar projeto
- Entre em `https://zeabur.com`
- Clique em `Create Project`
- Conecte seu GitHub e selecione o repositório `producao-streamlit`

## 2) Deploy do serviço
- Escolha `Deploy from GitHub`
- O Zeabur vai detectar o `Dockerfile` automaticamente
- Aguarde o build finalizar

## 3) Variáveis de ambiente
No serviço, adicione:

- `USE_FIREBASE=true`
- `FIREBASE_SERVICE_ACCOUNT_JSON=<JSON completo da chave da conta de serviço>`
- `FIREBASE_CACHE_TTL_SECONDS=3` (opcional)

## 4) Porta
- Não precisa configurar manualmente no app.
- O `Dockerfile` já usa `PORT` do Zeabur automaticamente.

## 5) Teste
- Abra a URL pública gerada pelo Zeabur
- Faça login e valide:
  - criação de pedido
  - mover etapas
  - relatório de finalizados
