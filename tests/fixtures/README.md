# Fixtures de integração

Três brokers Redpanda (`v25.3.18`) para os testes de conexão do `kafcli`.

| Serviço | Porta (host) | Admin (host) | Autenticação | Credenciais |
|---|---|---|---|---|
| `redpanda-plaintext` | 19092 | 19644 | nenhuma | — |
| `redpanda-scram` | 29092 | 29644 | SASL_PLAINTEXT, SCRAM-SHA-256 | usuário `kafcli`, senha `kafcli-it-scram-secret` |
| `redpanda-mtls` | 39092 | 39644 | TLS com certificado de cliente obrigatório | `certificates/` |

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

## Certificados

`generate-certificates.sh` gera uma CA própria e os certificados de servidor e de cliente em `tests/fixtures/certificates/` (PEM). O diretório está no `.gitignore`: chaves geradas nunca são commitadas. Os certificados são descartáveis e valem para uso local e de CI apenas.

## Observações

- Os brokers iniciam com `--mode dev-container --smp 1`, que desliga o fsync: são descartáveis e não servem para medir desempenho nem para guardar dados.
- O `rpk` não consegue reescrever um arquivo de configuração montado em modo leitura; por isso o `compose.yaml` copia a configuração para o diretório de dados antes de iniciar o broker.
- Para ver os logs de um broker: `docker compose -f tests/fixtures/compose.yaml logs redpanda-scram`.
