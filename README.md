# Processamento distribuído de apostas

API Go 1.27 com PostgreSQL 17, Keycloak, SQS no LocalStack e composição Uber Fx. Valores financeiros são representados em centavos inteiros; HTTP e SQS usam o mesmo caso de uso.

## Iniciar a partir de um checkout limpo

Pré-requisitos: Docker com Compose, Go 1.27 para executar comandos no host e portas 5432, 4566, 8080 e 8081 livres. Copie `.env.example` para `.env`; os valores são exclusivos do ambiente local e devem ser trocados fora dele. O `.env` é ignorado pelo Git.

```sh
cp .env.example .env
docker compose up --build -d
docker compose ps
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

Para executar o mesmo Compose com filas SQS reais definidas no `.env`, use `docker compose -f compose.yaml -f compose.aws.yaml up --build -d`. O arquivo base continua apontando a API para o LocalStack.

No PowerShell, use `Copy-Item .env.example .env` e `Invoke-RestMethod http://localhost:8080/health/ready`. O serviço `migrate` aplica as migrations antes da API. O script `deploy/localstack/init.sh` cria as filas FIFO de entrada, saída e DLQ e configura redrive com cinco recebimentos. A API aguarda SQS e JWKS durante a inicialização, por até 90 segundos. A fila de saída é `wager-events.fifo`.

O arquivo de exemplo contém apenas credenciais locais. Em produção, configure segredos externamente e deixe `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` vazios para usar a cadeia padrão de credenciais da AWS. A política mínima de SQS está em `deploy/aws/iam-policy.json`; seu escopo de ARN deve ser ajustado à conta e região de implantação. O LocalStack aceita as credenciais fictícias do exemplo e não prova enforcement de IAM da AWS.

## Identidades e chamadas HTTP

O realm `apostas` é importado automaticamente do Keycloak. Os clientes locais `wallet-service`, `provider-a` e `provider-b` usam `client_credentials` e segredos `<clientId>-local-only`. O `wallet-service` é a identidade interna para abertura, leitura e reconciliação de carteira. Cada provedor usa somente sua própria identidade e transações.

```sh
curl -s -X POST http://localhost:8081/realms/apostas/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=wallet-service \
  -d client_secret=wallet-service-local-only
```

Extraia `access_token` da resposta e envie `Authorization: Bearer <token>`. Faça o mesmo com `provider-a` e `provider-a-local-only` para apostas.

```sh
curl -X POST http://localhost:8080/wallets \
  -H 'Authorization: Bearer <wallet-token>' -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'

curl -X POST http://localhost:8080/wagering/transactions \
  -H 'Authorization: Bearer <provider-a-token>' -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-1' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-1","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<id-da-carteira>","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

As rotas de consulta são `GET /wallets/{walletId}`, `GET /wallets/{walletId}/ledger?limit=50&cursor=...`, `GET /wagering/transactions/{transactionId}` e `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`. `POST /wallets/{walletId}/reconciliation` reconstrói o saldo, sem alterá-lo. `/health/live` e `/health/ready` são públicos; `/metrics` exige a identidade interna. O cursor do ledger é opaco ao cliente, em ordem estável por instante e ID.

Resultados de aposta usam `transactionId`, `status`, `balance`, `idempotentReplay` e, em rejeições, `failureCode`. Códigos HTTP: `200` processada/replay, `202` referência pendente, `422` regra de negócio rejeitada, `400 {"code":"INVALID_INPUT"}`, `401` token ausente/inválido, `403 {"code":"FORBIDDEN"}`, `404 {"code":"NOT_FOUND"}`, `409 {"code":"CONFLICT"}` e `503 {"code":"TEMPORARY_UNAVAILABLE"}`. Abertura de carteira retorna `201`; duplicata retorna `409`. Os códigos de falha financeira são `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `WALLET_MISMATCH`, `INVALID_REFERENCE`, `REFERENCE_NOT_FOUND` e `AMOUNT_OVERFLOW`.

## Mensageria, migrations e testes

Envie a `wager-transactions.fifo` um envelope `WagerTransactionRequested` com `messageId` estável, `occurredAt` e `data` com os mesmos campos da operação HTTP e `idempotencyKey`. Use `MessageGroupId=<walletId>` e um `MessageDeduplicationId` da tentativa; o banco também deduplica pelo `messageId` do envelope e pela identidade financeira. Eventos publicados em `wager-events.fifo` usam `MessageGroupId=<walletId>` e `MessageDeduplicationId=<eventId>`. Consumidores devem deduplicar `eventId`, pois a entrega é pelo menos uma vez.

Para aplicar migrations manualmente no host, carregue `DATABASE_URL` do `.env` e execute `go run ./cmd/migrate up`. `go run ./cmd/migrate down` reverte apenas a versão aplicada mais recente; repetir reverte as anteriores. O runner registra versões em `schema_migrations` e usa advisory lock. **A reversão de `000001` apaga as tabelas e dados financeiros; use banco descartável para testá-la.** O Compose executa `up` automaticamente. Para recriar o ambiente local do zero, use um volume novo de PostgreSQL.

```sh
go test ./...
go test -race ./...
go vet ./...
docker build --target test .
```

No Windows, `go test -race` requer CGO e um compilador C. `docker build --target test .` executa o mesmo conjunto de testes com detector de corrida no Linux. Os testes de integração que precisam de serviços reais ficam desativados sem `TEST_DATABASE_URL`; `TEST_FX=1` e `TEST_E2E=1` habilitam os respectivos cenários. Inicie o Compose, exporte `TEST_DATABASE_URL` com a URL de host do `.env` e execute:

```sh
TEST_FX=1 TEST_E2E=1 go test -count=1 ./cmd/api ./internal/infra/postgres ./internal/integration
```

No PowerShell, atribua as variáveis com `$env:TEST_DATABASE_URL=...`, `$env:TEST_FX='1'` e `$env:TEST_E2E='1'`. Os testes de PostgreSQL criam schemas isolados, aplicam as migrations reais e removem os schemas ao terminar. Cobrem três processos de SO separados com suas próprias conexões, 50 envios simultâneos da mesma aposta, disputa 80/80 sobre saldo 100, carteiras independentes, proteção do ledger, inbox, referências pendentes e recuperação de claim da outbox. O teste E2E usa Keycloak e SQS reais, incluindo reenvio HTTP/SQS e mensagem inválida na DLQ.

Para simular interrupção após o commit e antes da remoção da mensagem, publique com `awslocal sqs send-message`, interrompa a API (`docker compose stop api`) e reinicie (`docker compose start api`). O processamento persistido é reconsultado pelo reenvio. Para simular publisher interrompido, pare a API após criar uma aposta, consulte `outbox_events` e reinicie; claims abandonados expiram em 30 segundos. O PostgreSQL pode ser interrompido com `docker compose stop postgres` e retomado com `docker compose start postgres`; erros transitórios deixam mensagens para retry. A seção de limitações em [ARCHITECTURE.md](ARCHITECTURE.md) distingue os cenários cobertos automaticamente dos passos manuais.

## Contratos e decisões

[ARCHITECTURE.md](ARCHITECTURE.md) descreve dinheiro, transações, locks, garantias SQL, idempotência, reversões, eventos, segurança, shutdown e limitações.
