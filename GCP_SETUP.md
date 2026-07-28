# Setup no Google Cloud (via Console)

Passo a passo para colocar o pipeline no ar usando o [console.cloud.google.com](https://console.cloud.google.com), sem usar o `gcloud` no terminal.

Nomes usados no repo (ajuste se for usar outros):
- Projeto: nome `etl-teste-tecnico`, ID real `etl-teste-tecnico-503722`
- Região: `us-east1`
- Bucket de dados de entrada: `landing-raw`
- Ambiente Composer: `composer-teste-tec-etl`
- Service account: `etl-runner`

> `PROJECT_ID = "etl-teste-tecnico-503722"` está fixo em todos os arquivos de [src/](src/). O **Nome do projeto** que você digita na criação nem sempre vira o **ID do projeto** — se o nome já estiver em uso por outra conta, o Google acrescenta um sufixo numérico automaticamente (foi o que aconteceu aqui). Depois de criar o projeto, confira o ID real em **IAM e admin** → **Configurações** e use esse valor exato no código, não o nome.

## 1. Criar o projeto

1. No topo da página, clique no seletor de projeto → **Novo Projeto**.
2. Nome do projeto: `etl-teste-tecnico`.
3. Selecione a conta de faturamento (billing account) vinculada.
4. Clique em **Criar**.
5. Depois de criado, vá em **IAM e admin** → **Configurações** e anote o **ID do projeto** exibido (pode vir com sufixo, ex: `etl-teste-tecnico-503722`) — é esse valor que entra em `PROJECT_ID` no código, não o nome.

## 2. Habilitar as APIs necessárias

Menu ☰ → **APIs e serviços** → **Biblioteca**. Busque e habilite (uma por vez, botão "Ativar"):
- **Cloud Storage API**
- **BigQuery API**
- **Cloud Composer API**
- **Cloud Functions API**
- **Cloud Build API**
- **Artifact Registry API**
- **Cloud Run Admin API** — o Cloud Functions 2ª geração roda sobre o Cloud Run; sem essa API o deploy falha com "Cloud Run Admin API has not been used in project..."
- **Compute Engine API** — habilitar cria automaticamente a *Compute Engine default service account* (`NUMERO_DO_PROJETO-compute@developer.gserviceaccount.com`), necessária pro build do Cloud Functions. Sem ela, o deploy falha com "missing permission on the build service account".

Depois de habilitar a Compute Engine API, conceda o papel **Cloud Build Service Account** (`roles/cloudbuild.builds.builder`) pra essa service account: **IAM e admin** → **IAM** → marque **Incluir concessões de papel fornecidas pelo Google** → encontre `NUMERO_DO_PROJETO-compute@developer.gserviceaccount.com` → editar → **Adicionar outro papel** → **Cloud Build Service Account** → **Salvar**.

## 3. Criar o bucket de Storage

Menu ☰ → **Cloud Storage** → **Buckets** → **Criar**.
- Nome: `landing-raw`
- Tipo de local: **Região** → `us-east1`
- Classe de armazenamento: Standard
- Controle de acesso: uniforme (padrão)
- Criar.

Depois, entre no bucket criado → **Fazer upload de arquivos** → selecione o CSV (`data/raw/df_fraud_credit_2.csv`).

> Os datasets do BigQuery (`empresa_raw`, `empresa_bronze`, `empresa_silver`, `empresa_gold`, `empresa_quality`) não precisam ser criados manualmente — o próprio pipeline cria na primeira execução.

## 4. Criar a Service Account

Menu ☰ → **IAM e administrador** → **Contas de serviço** → **Criar conta de serviço**.
- Nome: `etl-runner`
- Em "Conceder acesso a este projeto", adicione os papéis:
  - **BigQuery Data Editor**
  - **BigQuery Job User**
  - **Storage Object Admin**
- Clique em **Concluído**.

> Não baixe uma chave JSON para essa conta — políticas de organização costumam bloquear a criação de chaves (`iam.disableServiceAccountKeyCreation`), e o Composer não precisa de chave (usa a service account por referência). Para autenticar o GitHub Actions sem chave, veja a seção [Workload Identity Federation](#workload-identity-federation-github-actions-sem-chave) no final deste documento.

## 5. Criar o ambiente Cloud Composer (Airflow)

Menu ☰ → **Composer** → **Criar ambiente** → **Composer 2**.
- Nome: `composer-teste-tec-etl`
- Local: `us-east1`
- Versão da imagem: uma versão **Composer 2 / Airflow 2** (a mais recente estável)
- Conta de serviço do ambiente: selecione `etl-runner`
- Deixe o resto padrão e clique em **Criar**.

Isso leva uns 20-25 minutos para ficar pronto.

## 6. Instalar as dependências Python no Composer

Depois que o ambiente estiver criado, abra-o na lista → aba **PACOTES PYPI** → **Editar**.
Adicione, um por linha, o conteúdo de [requirements.txt](requirements.txt):
```
pandas>=2.0.0
google-cloud-bigquery>=3.0.0
python-dotenv>=1.0.0
gcsfs>=2023.1.0
pyarrow>=10.0.0
db-dtypes>=1.0.0
functions-framework>=3.0.0
```
Salvar (o ambiente reinicia os workers, leva alguns minutos).

## 7. Descobrir o bucket do Composer e publicar os DAGs

Na página do ambiente Composer → aba **CONFIGURAÇÃO DO AMBIENTE**, procure **Pasta de DAGs** — é um link para um bucket tipo `gs://us-east1-composer-teste-tec-XXXXXXXX-bucket/dags`.

Clique no link, isso abre o Cloud Storage nesse bucket:
- Dentro de `dags/`, faça **upload** de todos os arquivos de [dags/](dags/) (`dag_empresa.py`, `dag_empresa_gold_tabela_1.py`, `dag_empresa_gold_tabela_2.py`).
- Crie a subpasta `dags/src/` e faça upload de todo o conteúdo de [src/](src/) lá dentro.

## 8. Ver e ativar a DAG

Na página do ambiente Composer → clique em **Abrir link do Airflow** (abre a UI web do Airflow). Em alguns minutos a DAG `empresa_etl_pipeline` aparece na lista — ative o toggle (fica desligada por padrão) e ela roda no cron configurado (`0 10 * * *`), ou clique em **Trigger DAG** (ícone de play) para rodar manualmente.

---

## Autenticação do GitHub Actions com chave de service account

Necessário só se for usar o workflow [.github/workflows/run_pipeline.yml](.github/workflows/run_pipeline.yml), que autentica via secret `GCP_SA_KEY` (chave JSON). Por padrão, políticas de segurança da organização bloqueiam a criação de chaves (`iam.disableServiceAccountKeyCreation`) — os passos 9 e 10 mostram como liberar isso.

### 9. Desativar a política que bloqueia a criação de chaves

Menu ☰ → **IAM e admin** → **Políticas da organização**. Busque a política **Desativar a criação de chaves de conta de serviço** (`iam.disableServiceAccountKeyCreation`).
- Clique nela → **Gerenciar política**
- Em "Aplicação", selecione **Substituir a política do pai**
- Adicione uma regra com Aplicação = **Desativada**
- **Definir política**

> Só funciona se sua conta tiver o papel **Administrador de políticas da organização** (`roles/orgpolicy.policyAdmin`). Se não tiver, peça para quem administra a organização liberar essa política para o projeto `etl-teste-tecnico-503722`.

### 10. Gerar a chave JSON

Menu ☰ → **IAM e admin** → **Contas de serviço** → clique em `etl-runner` → aba **Chaves** → **Adicionar chave** → **Criar nova chave** → tipo **JSON** → **Criar**. O arquivo baixa automaticamente para o seu computador.

> Guarde esse arquivo com cuidado e nunca faça commit dele no repositório.

### 11. Cadastrar o secret no GitHub

No repositório do GitHub → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.
- Nome: `GCP_SA_KEY`
- Valor: cole o conteúdo inteiro do arquivo JSON baixado no passo 10
- **Add secret**

Depois de cadastrado, pode apagar o arquivo JSON local — o workflow já está configurado para usar esse secret via `credentials_json` no step "Authenticate to Google Cloud".
