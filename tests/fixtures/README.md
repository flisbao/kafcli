# Fixtures de integração

Três brokers Redpanda (`v25.3.18`) para os testes de conexão do `kafcli`.

| Serviço | Porta (host) | Admin (host) | Autenticação | Credenciais |
|---|---|---|---|---|
| `redpanda-plaintext` | 19092 | 19644 | nenhuma | — |
| `redpanda-scram` | 29092 | 29644 | SASL_PLAINTEXT, SCRAM-SHA-256 | usuário `kafcli`, senha `kafcli-it-scram-secret` |
| `redpanda-mtls` | 39092 | 39644 | TLS com certificado de cliente obrigatório | `certificates/` |
| `kafka-oauth` | 49092 | — | SASL_PLAINTEXT, OAUTHBEARER (JWT sem assinatura, `alg: none`) | token em `KAFCLI_IT_BEARER_TOKEN`, gerado pelo próprio teste |

`kafka-oauth` é um Apache Kafka 4.1.1 em KRaft com um nó só. O listener `EXTERNAL` não tem callback handler, então o broker usa o validador "unsecured": aceita qualquer JWT com `alg: none` e `exp`. Nunca use esse broker fora de testes.

O serviço `redpanda-scram-bootstrap` cria o usuário `kafcli`, torna-o superusuário e liga o SASL no cluster SCRAM. Depois fica no ar (`sleep infinity`) e o healthcheck só passa quando o bootstrap termina; é nele que `up --wait` se baseia.

## Subir e rodar os testes

A partir da raiz do repositório:

```bash
bash tests/fixtures/generate-certificates.sh
docker compose -f tests/fixtures/compose.yaml up -d --wait
KAFCLI_IT_SCRAM_PASSWORD=kafcli-it-scram-secret \
  cargo test -p kafcli --test integration_connection -- --ignored
docker compose -f tests/fixtures/compose.yaml down -v
```

O comando `down -v` remove os containers e os volumes; ele deve rodar mesmo quando os testes falham.

## Testes OAuth

Usam só o broker `kafka-oauth` (`127.0.0.1:49092`). Os testes criam um JWT `alg: none` com `sub` e `exp` próprios e criam e apagam os tópicos dentro do container.

```bash
docker compose -f tests/fixtures/compose.yaml up -d --wait kafka-oauth
cargo test -p kafcli --test integration_oauth_cli bearer -- --ignored --test-threads=1
docker compose -f tests/fixtures/compose.yaml down -v
```

## Testes de produce e consume

Usam só o broker plaintext (`127.0.0.1:19092`). Criam tópicos com `rpk` dentro do container e os removem ao final, mesmo quando um teste falha.

```bash
docker compose -f tests/fixtures/compose.yaml up -d --wait
cargo test -p kafcli --test integration_produce_consume_cli -- --ignored --test-threads=1
cargo test -p kafcli-core --test integration_produce_consume -- --ignored --test-threads=1
docker compose -f tests/fixtures/compose.yaml down -v
```

- `integration_produce_consume_cli` executa o binário `kafcli` de verdade: ida e volta de `consume --output json` para `produce --input envelope --stdin`, valor não-JSON preservado como `value_base64`, fechamento precoce do pipe (`| head`) com saída 0 e stderr vazio, e neutralização de caracteres de controle na saída em tabela.
- `integration_produce_consume` (núcleo) cobre a API de `kafcli-core`: offsets, `--count`, `--from`, truncamento, filtros, redação e o consumo sem commit.
- `--test-threads=1` evita que os testes disputem o broker ao mesmo tempo; os testes não dependem da ordem entre si.

## Testes de schemas (Avro e JSON Schema)

O broker plaintext também expõe o Schema Registry em `127.0.0.1:18081` (API compatível com o Confluent). Os testes `integration_schemas_cli` registram os schemas com o cliente do núcleo, nunca com `produce`, e esperam `GET /subjects` responder 200 por até 30 s.

```bash
docker compose -f tests/fixtures/compose.yaml up -d --wait
cargo test -p kafcli --test integration_schemas_cli -- --ignored --test-threads=1
cargo test -p kafcli --test integration_produce_consume_cli -- --ignored --test-threads=1
docker compose -f tests/fixtures/compose.yaml down -v
```

- Os casos Confluent usam o registry do próprio broker; os casos Azure usam um servidor local de teste no lugar do registry, em `http://`, com um token `bearer` fictício.
- O teste de Avro profundamente aninhado grava os bytes diretamente pelo núcleo, porque `produce` recusa JSON com mais de 64 níveis.
- Os subjects e tópicos têm nomes únicos por execução e os tópicos são removidos ao final, mesmo quando um teste falha.

## Certificados

`generate-certificates.sh` gera uma CA própria e os certificados de servidor e de cliente em `tests/fixtures/certificates/` (PEM). O diretório está no `.gitignore`: chaves geradas nunca são commitadas. Os certificados são descartáveis e valem para uso local e de CI apenas.

## Observações

- Os brokers iniciam com `--mode dev-container --smp 1`, que desliga o fsync: são descartáveis e não servem para medir desempenho nem para guardar dados.
- O `rpk` não consegue reescrever um arquivo de configuração montado em modo leitura; por isso o `compose.yaml` copia a configuração para o diretório de dados antes de iniciar o broker.
- Para ver os logs de um broker: `docker compose -f tests/fixtures/compose.yaml logs redpanda-scram`.
