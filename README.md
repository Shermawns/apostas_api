# Processamento distribuído de apostas

API em Go para abrir carteiras, registrar apostas e seus resultados, manter um ledger financeiro auditável e processar comandos assíncronos pelo Amazon SQS. O serviço usa PostgreSQL como fonte de verdade, Keycloak para autenticação OIDC e o padrão outbox para publicar eventos somente depois do commit financeiro.

## Componentes

- API HTTP em `http://localhost:8080`.
- PostgreSQL para carteiras, transações, ledger, inbox e outbox.
- Keycloak em `http://localhost:8081`.
- LocalStack em `http://localhost:4566` para executar SQS localmente.
- Worker de entrada SQS, publisher da outbox e resolver de referências pendentes no mesmo processo da API.
- Uber Fx para composição e ciclo de vida.

## Pré-requisitos

As versões declaradas pelo projeto são:

| Componente | Versão |
|---|---:|
| Go | 1.27 (`go.mod` e imagem de build) |
| PostgreSQL | 17 Alpine |
| Keycloak | 26.3 |
| LocalStack | 4.8 |
| Alpine da imagem final | 3.22 |

Para o caminho recomendado, instale Docker Engine ou Docker Desktop com Docker Compose v2. O repositório não fixa uma versão mínima do Docker; ele requer suporte a `docker compose`, health checks e `depends_on.condition`. Para executar comandos no host, instale Go 1.27. Os exemplos HTTP usam `curl`; os exemplos Bash usam `jq` apenas para extrair tokens e IDs.

Verifique o ambiente:

```bash
docker version
docker compose version
go version
curl --version
```

Portas locais usadas por padrão: `5432`, `4566`, `8080` e `8081`. Se `5432` estiver ocupada, altere `POSTGRES_HOST_PORT` no `.env`, por exemplo para `5433`, e ajuste a porta da `DATABASE_URL` usada no host.

## Configuração

Crie seu arquivo local de configuração:

```bash
cp .env.example .env
```

No PowerShell:

```powershell
Copy-Item .env.example .env
```

O `.env` está ignorado pelo Git. O arquivo `.env.example` contém somente credenciais fictícias do ambiente local.

| Variável | Obrigatória | Finalidade |
|---|---:|---|
| `APP_ADDR` | sim | Endereço em que a API escuta, normalmente `:8080`. |
| `APP_SHUTDOWN_TIMEOUT` | sim | Prazo do shutdown gracioso aceito por `time.ParseDuration`, por exemplo `20s`. |
| `DATABASE_URL` | sim | DSN PostgreSQL usado quando a aplicação ou os testes rodam no host. |
| `POSTGRES_HOST_PORT` | Compose | Porta do PostgreSQL exposta no host. |
| `POSTGRES_DB` | Compose | Banco criado pelo container PostgreSQL. |
| `POSTGRES_USER` | Compose | Usuário local do PostgreSQL. |
| `POSTGRES_PASSWORD` | Compose | Senha local do PostgreSQL. |
| `AWS_REGION` | sim | Região usada pelo SDK AWS. |
| `AWS_ENDPOINT_URL` | não | Endpoint alternativo. Use `http://localhost:4566` no host, `http://localstack:4566` no Compose e vazio para AWS real. |
| `AWS_ACCESS_KEY_ID` | condicional | Credencial explícita. O LocalStack aceita `test`; na AWS pode ficar vazia para usar a cadeia padrão do SDK. |
| `AWS_SECRET_ACCESS_KEY` | condicional | Deve ser informada junto com `AWS_ACCESS_KEY_ID`. Nunca a versione. |
| `AWS_SQS_INPUT_QUEUE_URL` | sim | URL de `wager-transactions.fifo`. |
| `AWS_SQS_OUTPUT_QUEUE_URL` | sim | URL de `wager-events.fifo`. |
| `AWS_SQS_DLQ_URL` | sim | URL de `wager-transactions-dlq.fifo`. |
| `OIDC_ISSUER_URL` | sim | Issuer esperado no JWT. |
| `OIDC_JWKS_URL` | sim | Endpoint das chaves públicas do Keycloak. |
| `OIDC_AUDIENCE` | sim | Audience exigida no token, `apostas-api` no ambiente local. |
| `KEYCLOAK_ADMIN` | Compose | Administrador de desenvolvimento do Keycloak. Não autentica chamadas de negócio. |
| `KEYCLOAK_ADMIN_PASSWORD` | Compose | Senha do administrador local do Keycloak. |
| `MIGRATIONS_DIR` | migrate | Diretório das migrations; o padrão é `migrations`. |
| `TEST_DATABASE_URL` | testes | Banco real usado pelos testes PostgreSQL, Fx e E2E. |
| `TEST_FX` | testes | Use `1` para habilitar o teste do ciclo de vida Fx. |
| `TEST_E2E` | testes | Use `1` para habilitar o teste HTTP, Keycloak e SQS. |

## Subir todo o ambiente local

O fluxo local recomendado é:

```bash
docker compose up --build -d
docker compose ps
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

Respostas esperadas:

```json
{"status":"live"}
{"status":"ready"}
```

Durante a subida:

1. O PostgreSQL cria o banco configurado.
2. O serviço `migrate` aplica todas as migrations antes da API.
3. `deploy/localstack/init.sh` cria as filas FIFO `wager-transactions.fifo`, `wager-events.fifo` e `wager-transactions-dlq.fifo`.
4. A fila de entrada recebe visibility timeout de 30 segundos e redrive para a DLQ após cinco recebimentos.
5. O Keycloak importa `deploy/keycloak/realm.json` e cria as identidades de teste.
6. A API valida PostgreSQL, JWKS e as três filas antes de ficar pronta.

Confira as filas:

```bash
docker compose exec localstack awslocal sqs list-queues
docker compose exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --attribute-names All
```

Para acompanhar logs:

```bash
docker compose logs -f api migrate localstack keycloak
```

Para encerrar sem apagar os dados:

```bash
docker compose down
```

`docker compose down -v` também remove o volume PostgreSQL e todos os dados; use somente quando quiser recriar o ambiente.

### Usar filas reais da AWS

Preencha no `.env` a região, URLs reais das três filas e credenciais válidas, ou configure a cadeia padrão de credenciais do SDK. Deixe `AWS_ENDPOINT_URL` vazio e execute:

```bash
docker compose -f compose.yaml -f compose.aws.yaml up --build -d
```

A policy mínima está em `deploy/aws/iam-policy.json`. Ajuste os ARNs para sua conta e região. O override altera somente a configuração SQS da API; os demais serviços locais continuam disponíveis.

## Keycloak e tokens

O realm `apostas` é importado automaticamente. Ele contém três clients confidenciais com service account:

| Client | Segredo local | Acesso |
|---|---|---|
| `wallet-service` | `wallet-service-local-only` | Carteiras, ledger, reconciliação, métricas e consulta interna de transações. |
| `provider-a` | `provider-a-local-only` | Envio e consulta das próprias transações. |
| `provider-b` | `provider-b-local-only` | Envio e consulta das próprias transações. |

Obtenha tokens em Bash:

```bash
WALLET_TOKEN=$(curl -fsS -X POST http://localhost:8081/realms/apostas/protocol/openid-connect/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d grant_type=client_credentials \
  -d client_id=wallet-service \
  -d client_secret=wallet-service-local-only | jq -r .access_token)

PROVIDER_TOKEN=$(curl -fsS -X POST http://localhost:8081/realms/apostas/protocol/openid-connect/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d grant_type=client_credentials \
  -d client_id=provider-a \
  -d client_secret=provider-a-local-only | jq -r .access_token)
```

No PowerShell:

```powershell
$walletToken = (curl.exe -s -X POST http://localhost:8081/realms/apostas/protocol/openid-connect/token `
  -H "Content-Type: application/x-www-form-urlencoded" `
  -d "grant_type=client_credentials" -d "client_id=wallet-service" `
  -d "client_secret=wallet-service-local-only" | ConvertFrom-Json).access_token

$providerToken = (curl.exe -s -X POST http://localhost:8081/realms/apostas/protocol/openid-connect/token `
  -H "Content-Type: application/x-www-form-urlencoded" `
  -d "grant_type=client_credentials" -d "client_id=provider-a" `
  -d "client_secret=provider-a-local-only" | ConvertFrom-Json).access_token
```

O middleware aceita apenas JWT RS256 com issuer, audience e expiração válidos. A identidade usada na autorização vem do claim `azp`.

## Fluxo HTTP completo

Os exemplos abaixo usam Bash, os tokens anteriores e identificadores fixos. Em um banco já utilizado, troque `PLAYER_ID` para evitar conflito de `(playerId, currency)`.

```bash
PLAYER_ID=0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1
```

### 1. Abrir uma carteira

```bash
WALLET_RESPONSE=$(curl -fsS -X POST http://localhost:8080/wallets \
  -H "Authorization: Bearer $WALLET_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER_ID\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")

echo "$WALLET_RESPONSE" | jq
WALLET_ID=$(echo "$WALLET_RESPONSE" | jq -r .id)
```

Resposta `201 Created`:

```json
{
  "id": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "balance": {"amount": "100.00", "currency": "BRL"},
  "version": 1
}
```

Saldo inicial positivo cria `OPENING`, crédito no ledger e dois eventos na outbox. Saldo inicial `0.00` cria somente a carteira.

### 2. Enviar uma BET

```bash
BET_RESPONSE=$(curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-001' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-001\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-001\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}")

echo "$BET_RESPONSE" | jq
BET_TRANSACTION_ID=$(echo "$BET_RESPONSE" | jq -r .transactionId)
```

### 3. Enviar uma WIN

WIN pode ser independente ou referenciar uma BET. Este exemplo referencia a BET:

```bash
WIN_RESPONSE=$(curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:win-001' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"win-001\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-001\",\"gameId\":\"fortune-chimp\",\"kind\":\"WIN\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"bet-001\"}")

echo "$WIN_RESPONSE" | jq
```

### 4. Enviar uma LOSS

LOSS encerra o resultado sem movimentar dinheiro; seu valor deve ser `0.00`.

```bash
curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:loss-001' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"loss-001\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-002\",\"gameId\":\"fortune-chimp\",\"kind\":\"LOSS\",\"money\":{\"amount\":\"0.00\",\"currency\":\"BRL\"}}" | jq
```

### 5. Enviar um REFUND

REFUND só aceita uma BET processada, com o mesmo valor integral e os mesmos provider, jogador, carteira, rodada e moeda.

```bash
curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:refund-001' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"refund-001\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-001\",\"gameId\":\"fortune-chimp\",\"kind\":\"REFUND\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"bet-001\"}" | jq
```

### 6. Enviar um ROLLBACK

Uma referência só pode ter uma reversão processada entre REFUND e ROLLBACK. Para testar ROLLBACK separadamente, crie outra BET:

```bash
curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-rollback-001' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-rollback-001\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-003\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"5.00\",\"currency\":\"BRL\"}}" | jq

curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:rollback-001' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"rollback-001\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-003\",\"gameId\":\"fortune-chimp\",\"kind\":\"ROLLBACK\",\"money\":{\"amount\":\"5.00\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"bet-rollback-001\"}" | jq
```

### 7. Consultar carteira

```bash
curl -fsS "http://localhost:8080/wallets/$WALLET_ID" \
  -H "Authorization: Bearer $WALLET_TOKEN" | jq
```

### 8. Consultar ledger paginado

```bash
FIRST_PAGE=$(curl -fsS "http://localhost:8080/wallets/$WALLET_ID/ledger?limit=2" \
  -H "Authorization: Bearer $WALLET_TOKEN")
echo "$FIRST_PAGE" | jq

CURSOR=$(echo "$FIRST_PAGE" | jq -r '.nextCursor // empty')
if [ -n "$CURSOR" ]; then
  curl -fsS "http://localhost:8080/wallets/$WALLET_ID/ledger?limit=2&cursor=$CURSOR" \
    -H "Authorization: Bearer $WALLET_TOKEN" | jq
fi
```

O limite padrão é 50 e o intervalo permitido é 1 a 100. O cursor é opaco, codificado em Base64 URL-safe e representa `(createdAt, id)`.

### 9. Consultar transação pelo ID interno

```bash
curl -fsS "http://localhost:8080/wagering/transactions/$BET_TRANSACTION_ID" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" | jq
```

`wallet-service` pode consultar qualquer transação. Um provedor só encontra suas próprias transações; uma transação de outro provedor retorna `404` nessa rota.

### 10. Consultar por provedor e ID externo

```bash
curl -fsS http://localhost:8080/providers/provider-a/wagering/transactions/bet-001 \
  -H "Authorization: Bearer $PROVIDER_TOKEN" | jq
```

### 11. Reconciliar carteira

```bash
curl -fsS -X POST "http://localhost:8080/wallets/$WALLET_ID/reconciliation" \
  -H "Authorization: Bearer $WALLET_TOKEN" | jq
```

A reconciliação não altera saldo. Ela compara o saldo armazenado com crédito menos débito do ledger em um único snapshot da consulta.

### 12. Health checks e métricas

```bash
curl -fsS http://localhost:8080/health/live
curl -fsS http://localhost:8080/health/ready
curl -fsS http://localhost:8080/metrics -H "Authorization: Bearer $WALLET_TOKEN"
```

Somente os health checks são públicos.

## Publicar uma mensagem SQS local

O payload SQS usa os mesmos campos de negócio do HTTP, com `idempotencyKey` dentro de `data`. O `messageId` identifica a entrega na inbox. Use o ID da carteira criada anteriormente:

```bash
SQS_BODY=$(printf '{"messageId":"message-bet-sqs-001","type":"WagerTransactionRequested","occurredAt":"2026-01-01T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"bet-sqs-001","idempotencyKey":"provider-a:bet-sqs-001","playerId":"%s","walletId":"%s","roundId":"round-sqs-001","gameId":"fortune-chimp","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}}' "$PLAYER_ID" "$WALLET_ID")

docker compose exec -T localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-body "$SQS_BODY" \
  --message-group-id "$WALLET_ID" \
  --message-deduplication-id message-bet-sqs-attempt-001
```

Use `MessageGroupId=<walletId>` para serialização FIFO por carteira. Use um identificador único da tentativa em `MessageDeduplicationId`; a deduplicação durável do negócio ocorre no PostgreSQL pelo `messageId`, pela chave de idempotência e pelo ID externo.

## Migrations

O Compose executa `migrate up` automaticamente. Para executar no host:

```bash
export DATABASE_URL='postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable'
go run ./cmd/migrate up
```

PowerShell:

```powershell
$env:DATABASE_URL = 'postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable'
go run ./cmd/migrate up
```

Cada execução de `down` reverte somente a migration aplicada mais recente:

```bash
go run ./cmd/migrate down
```

O runner usa a tabela `schema_migrations`, executa cada arquivo dentro de uma transação e adquire um advisory lock para impedir dois migradores simultâneos. Reverter `000001` apaga tabelas e dados financeiros; faça isso somente em banco descartável.

## Executar a API no host

Suba somente as dependências e as migrations:

```bash
docker compose up --build -d postgres localstack keycloak migrate
go run ./cmd/api
```

Neste modo, as URLs do `.env` devem apontar para `localhost`. Para rodar tudo em containers, use `docker compose up --build -d`.

## Testes

### Testes rápidos

```bash
go test ./...
go vet ./...
```

Sem as variáveis opt-in, os testes que dependem de serviços externos são ignorados. Não há build tags neste projeto.

### Detector de corrida

```bash
go test -race ./...
```

No Windows, `-race` exige CGO e compilador C. O caminho reproduzível em Linux é a etapa de teste da imagem:

```bash
docker build --target test -t apostas-api-test .
```

### Integração PostgreSQL, três processos e concorrência 80/80

Suba PostgreSQL e aplique migrations:

```bash
docker compose up --build -d postgres migrate
export TEST_DATABASE_URL='postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable'
go test -count=1 ./internal/infra/postgres
```

Esses testes criam schemas isolados e cobrem:

- três processos de sistema operacional com conexões e memória próprias;
- duas BET distintas de `80.00` sobre `100.00`, com saldo final `20.00` e um débito;
- 50 reenvios simultâneos da mesma operação;
- carteiras independentes processadas em paralelo;
- proteção do ledger e do saldo no banco;
- inbox, referência pendente, expiração e recuperação de claims da outbox.

Se o teste for executado dentro de um container e `TEST_DATABASE_URL` usa `localhost`, troque o host por `host.docker.internal`:

```powershell
$dockerTestUrl = $env:TEST_DATABASE_URL -replace 'localhost', 'host.docker.internal'
docker run --rm `
  -e "TEST_DATABASE_URL=$dockerTestUrl" `
  -v "${PWD}:/src" `
  -w /src `
  apostas-api-test `
  sh -c "CGO_ENABLED=1 go test -race -count=1 ./internal/infra/postgres"
```

### Ciclo de vida Fx

Com PostgreSQL, Keycloak e LocalStack ativos:

```bash
TEST_DATABASE_URL='postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable' \
TEST_FX=1 \
go test -count=1 ./cmd/api
```

O teste confirma que o app inicia com dependências reais, termina os workers antes de retornar do shutdown e fecha o pool PostgreSQL.

### E2E HTTP, Keycloak e SQS

O teste espera a API já em execução pelo Compose base, apontada para o LocalStack:

```bash
docker compose up --build -d
TEST_DATABASE_URL='postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable' \
TEST_E2E=1 \
go test -count=1 ./internal/integration
```

Ele valida autenticação, isolamento entre provedores, abertura, replay HTTP/SQS, concorrência entre HTTP e SQS, ledger único, publicação da outbox, DLQ e reconciliação.

### Todos os testes opt-in

```bash
export TEST_DATABASE_URL='postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable'
export TEST_FX=1
export TEST_E2E=1
go test -p 1 -count=1 ./cmd/api ./internal/infra/postgres ./internal/integration
```

PowerShell:

```powershell
$env:TEST_DATABASE_URL = 'postgres://apostas:apostas-local-only@localhost:5432/apostas?sslmode=disable'
$env:TEST_FX = '1'
$env:TEST_E2E = '1'
go test -p 1 -count=1 ./cmd/api ./internal/infra/postgres ./internal/integration
```

### Simular falhas e recuperação

Para observar reentrega após interrupção do consumidor:

1. Publique uma mensagem SQS válida com `MessageDeduplicationId` novo.
2. Acompanhe `docker compose logs -f api`.
3. Interrompa a API com `docker compose kill api` durante o processamento.
4. Aguarde o visibility timeout de 30 segundos e execute `docker compose up -d api`.
5. Consulte carteira, transação e ledger. A inbox e as constraints impedem um segundo efeito financeiro.

O código não possui um failpoint que permita parar deterministicamente no intervalo exato entre commit PostgreSQL e `DeleteMessage`; portanto, esse teste manual depende do momento da interrupção.

Para observar recuperação da outbox, crie uma operação e interrompa a API. Eventos não publicados ou claims abandonados voltam a ser elegíveis; o lease do claim expira em 30 segundos. Uma falha após `SendMessage` e antes de `published_at` pode republicar o mesmo `eventId`, então consumidores externos devem deduplicá-lo.

Para testar indisponibilidade do PostgreSQL:

```bash
docker compose stop postgres
docker compose start postgres
```

Mensagens com erro transitório não são apagadas da origem e são retentadas. Após cinco recebimentos, o worker tenta enviá-las para a DLQ.

## Códigos HTTP principais

| Situação | HTTP | Corpo |
|---|---:|---|
| Sucesso ou replay processado | 200 | Resultado ou recurso solicitado |
| Carteira criada | 201 | Carteira |
| Referência pendente | 202 | Resultado com `status=PENDING_REFERENCE` |
| JSON, Money, UUID ou cursor inválido | 400 | `{"code":"INVALID_INPUT"}` |
| Token ausente ou inválido | 401 | Texto do middleware OIDC |
| Identidade sem permissão | 403 | `{"code":"FORBIDDEN"}` |
| Recurso não encontrado | 404 | `{"code":"NOT_FOUND"}` |
| Chave/ID externo conflitante ou unicidade | 409 | `{"code":"CONFLICT"}` |
| Regra financeira rejeitada | 422 | Resultado com `status=REJECTED` e `failureCode` |
| PostgreSQL ou dependência indisponível | 503 | `{"code":"TEMPORARY_UNAVAILABLE"}` ou código específico no readiness |

## Troubleshooting

### `go.mod file not found`

Execute comandos Go na raiz do repositório:

```powershell
Set-Location 'C:\Users\sherm\OneDrive\Documentos\apostas_api'
go test ./...
```

### Porta 5432 ocupada

Defina `POSTGRES_HOST_PORT=5433` e use `localhost:5433` na `DATABASE_URL` do host e em `TEST_DATABASE_URL`. Dentro do Compose, a API continua usando `postgres:5432`.

### `TEST_DATABASE_URL is required`

O teste opt-in não lê essa variável do `.env` automaticamente. Exporte-a no mesmo terminal antes de `go test`.

### Container de teste não conecta em `localhost`

Dentro de um container, `localhost` aponta para o próprio container. Use `host.docker.internal` para acessar o PostgreSQL publicado pelo host.

### `/health/ready` retorna `SQS_UNAVAILABLE`

Confirme as três URLs de fila e execute `docker compose exec localstack awslocal sqs list-queues`. Para AWS real, deixe `AWS_ENDPOINT_URL` vazio e verifique região, credenciais e a policy IAM.

### `/health/ready` retorna `DATABASE_UNAVAILABLE`

Execute `docker compose ps postgres migrate` e confira `docker compose logs postgres migrate`.

### API não inicia enquanto o Keycloak sobe

O Fx aguarda JWKS por até 90 segundos. Confira `docker compose logs keycloak api` e valide `http://localhost:8081/realms/apostas/protocol/openid-connect/certs`.

### `401` com token aparentemente válido

O token precisa ser RS256, não expirado, emitido por `OIDC_ISSUER_URL`, conter audience `apostas-api` e possuir `azp` com o client correto.

### `403` em rota de carteira

Use o token de `wallet-service`. Tokens `provider-a` e `provider-b` não podem abrir, ler ou reconciliar carteiras.

## Arquitetura

As decisões, garantias, máquina de estados, contratos, eventos, diagrama de fluxo e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md).
