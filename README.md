# SDCP-MOM – Leilão com Message-Oriented Middleware (RabbitMQ)

Prova de Conceito (POC) da disciplina **Computação Paralela e Sistemas Distribuídos** – **Grupo 3 – MOM**.

Um **Participante** envia um lance e o **Leiloeiro** o recebe, valida e responde **ACEITO** ou **RECUSADO**. Toda a comunicação passa pelo broker **RabbitMQ**.

## Integrantes

- Daniel Teles de Oliveira
- João Victor Torres Soares
- Pedro Gabriel Takenobu Serafim
- Rubens Rodrigues Maranesi

## Tecnologia

**Message-Oriented Middleware (MOM)** é uma camada de software que permite a aplicações distribuídas trocarem mensagens de forma assíncrona por meio de um intermediário (o *broker*), com desacoplamento espacial e temporal entre produtor e consumidor.

Usamos o **RabbitMQ**, broker open source escrito em Erlang que implementa o protocolo **AMQP 0-9-1**. O produtor publica em uma *exchange*, que roteia a mensagem para uma ou mais *filas*, de onde os consumidores a recebem.

Principais recursos usados na POC: exchange `direct`, filas duráveis, mensagens persistentes, ack manual do consumidor, prefetch, dead letter queue e o padrão request/reply (`reply_to` + `correlation_id`).

## Arquitetura

```
                      ┌──────────────── Broker RabbitMQ ────────────────┐
                      │                                                 │
 Participante ──(1)──►│ Exchange "leilao" ──(2)──► fila.lances          │──(3)──► Leiloeiro
 (produtor)           │      (direct)                                   │         (consumidor)
      ▲               │                                                 │             │
      │               │      respostas.<participante_id>  ◄─────────────│────(4)──────┘
      └──────(5)──────│      (fila exclusiva do participante)           │
                      └─────────────────────────────────────────────────┘
```

| Processo | Função | Inicia a comunicação? |
|---|---|---|
| Participante | Produtor de lances e consumidor das respostas | Sim |
| Leiloeiro | Consumidor de lances e produtor de respostas | Não, reage aos lances |
| Broker RabbitMQ | Roteia, armazena e entrega as mensagens | Não, é intermediário |

### Fluxo de mensagens

1. O Participante publica o lance na exchange `leilao` com routing key `lances`, informando `reply_to` e `correlation_id`.
2. A exchange roteia o lance para a fila durável `fila.lances`.
3. O Leiloeiro consome `fila.lances`, lê o lance e o valida.
4. O Leiloeiro publica a resposta na fila indicada em `reply_to`, repetindo o `correlation_id`, e só então confirma (ack) o lance.
5. O Participante consome sua fila de respostas e exibe o resultado.

### Exchanges e filas

| Elemento | Tipo | Produtor | Consumidor | Routing key |
|---|---|---|---|---|
| `leilao` | Exchange direct | Participante | – | `lances` |
| `fila.lances` | Fila durável | (via exchange) | Leiloeiro | `lances` (binding) |
| `respostas.<participante_id>` | Fila exclusiva do participante | Leiloeiro | Participante | nome da fila (exchange default) |
| `fila.lances.dlq` | Dead letter queue | (broker) | – | – |

## Formato das mensagens (JSON)

**Lance** (Participante → Leiloeiro)

```json
{
  "lance_id": "uuid",
  "item_id": "item-001",
  "participante_id": "participante-1",
  "valor": 150.00,
  "timestamp": "2026-10-30T10:00:00Z"
}
```

Propriedades AMQP: `reply_to = respostas.participante-1`, `correlation_id = lance_id`, `content_type = application/json`, `delivery_mode = 2` (persistente).

**Resposta** (Leiloeiro → Participante)

```json
{
  "lance_id": "uuid",
  "item_id": "item-001",
  "participante_id": "participante-1",
  "status": "ACEITO",
  "motivo": "Maior lance vigente",
  "maior_lance": 150.00,
  "timestamp": "2026-10-30T10:00:01Z"
}
```

O `lance_id` (também enviado como `correlation_id`) liga a resposta ao lance original, já que a comunicação é assíncrona.

## Decisões de design

- **Um único Leiloeiro consumindo a fila:** preserva a ordem FIFO dos lances e a consistência do maior lance de cada item.
- **Regra de validação:** o lance é aceito apenas se o valor for maior que o maior lance vigente do item.
- **Ack manual:** o Leiloeiro confirma o lance somente após processar e responder (semântica *at-least-once*); o `lance_id` permite descartar duplicatas.
- **Persistência:** fila durável e mensagens persistentes.
- **Prefetch = 1:** um lance por vez, em sequência.
- **Fila de respostas por participante:** cada participante recebe apenas as suas respostas.
- **Mensagens inválidas:** são rejeitadas sem reenfileirar e vão para `fila.lances.dlq`.

## Ambiente

- Broker: RabbitMQ em Docker (`rabbitmq:4-management`), um único nó.
- Portas: **5672** (AMQP) e **15672** (interface web de gerenciamento).
- Linguagem: Python com [`pika`](https://pika.readthedocs.io) *(a confirmar pelo grupo)*.
- Execução em duas máquinas: o broker roda em uma delas e a outra se conecta pelo IP dela. Libere a porta 5672 no firewall.
- O usuário padrão `guest` só aceita conexões locais, por isso o compose cria um usuário próprio.

`docker-compose.yml`:

```yaml
services:
  rabbitmq:
    image: rabbitmq:4-management
    container_name: rabbitmq-leilao
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: leilao
      RABBITMQ_DEFAULT_PASS: leilao123
```

## Como executar *(provisório – ajustar quando o código estiver pronto)*

```bash
# 1. Subir o broker (na máquina do broker)
docker compose up -d

# 2. Interface web: http://<IP_DO_BROKER>:15672  (usuário: leilao / senha: leilao123)

# 3. Instalar dependências (em cada máquina)
pip install pika

# 4. Iniciar o Leiloeiro
python leiloeiro.py --host <IP_DO_BROKER>

# 5. Enviar um lance com o Participante
python participante.py --host <IP_DO_BROKER> --item item-001 --valor 150
```

## Estrutura prevista do repositório

```
SDCP-MOM/
├── README.md
├── docker-compose.yml
├── leiloeiro.py
├── participante.py
└── docs/
    └── Relatorio_Fase_1.pdf
```

## Divisão de tarefas

| Integrante | Responsabilidade principal |
|---|---|
| Daniel Teles de Oliveira | Estudo da tecnologia (paradigma MOM e arquitetura do RabbitMQ) e redação do relatório |
| João Victor Torres Soares | Comparação com outras tecnologias, casos de uso e ferramentas |
| Pedro Gabriel Takenobu Serafim | Ambiente (Docker Compose, RabbitMQ, rede entre as máquinas) e criação de exchange, filas e usuário |
| Rubens Rodrigues Maranesi | Implementação do Participante e do Leiloeiro |
| Todos | Testes da primeira comunicação, revisão do README e do relatório |

## Evoluções previstas

- Múltiplos participantes simultâneos.
- Exchange `topic` com chaves por item (`lance.item-001`).
- Exchange `fanout` para avisar todos sobre o novo maior lance.
- Leilão com prazo de encerramento (TTL).
- Simulação de falhas (queda do Leiloeiro e reentrega das mensagens não confirmadas).
- Medição de latência e throughput.

## Referências

- [RabbitMQ – Documentação](https://www.rabbitmq.com/docs)
- [RabbitMQ –
