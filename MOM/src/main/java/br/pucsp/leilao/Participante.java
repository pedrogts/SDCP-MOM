package br.pucsp.leilao;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Participante {

    private static final ConcurrentHashMap<String, CountDownLatch> consultasPendentes =
            new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {

        String id = args.length > 0 ? args[0] : "participante-1";

        Connection conn = Config.factory().newConnection(id);

        // Canal para publicar mensagens
        Channel canalPublicacao = conn.createChannel();

        // Canal separado para consumir respostas
        Channel canalConsumo = conn.createChannel();

        Config.declararTopologia(canalPublicacao);

        String filaResposta = "respostas." + id;

        canalConsumo.queueDeclare(
                filaResposta,
                false,
                true,
                true,
                null
        );

        // Consumidor das respostas de lance e das consultas
        canalConsumo.basicConsume(
                filaResposta,
                true,
                (tag, d) -> {
                    String corpo = new String(
                            d.getBody(),
                            StandardCharsets.UTF_8
                    );

                    try {
                        JsonObject json = JsonParser.parseString(corpo)
                                .getAsJsonObject();

                        String tipo = json.has("tipo")
                                ? json.get("tipo").getAsString()
                                : "";

                        if ("ESTADO_LEILAO".equals(tipo)) {

                            EstadoLeilao estado =
                                    Config.GSON.fromJson(corpo, EstadoLeilao.class);

                            System.out.println();
                            System.out.println("========== ESTADO DO LEILÃO ==========");
                            System.out.println("Item: " + estado.itemId);
                            System.out.println("Situação: " + estado.status);
                            System.out.printf(
                                    "Maior lance: R$ %.2f%n",
                                    estado.maiorLance
                            );
                            System.out.println(
                                    "Maior lance dado por: " + estado.maiorLanceDe
                            );
                            System.out.println(
                                    "Tempo restante: "
                                            + estado.tempoRestanteSegundos
                                            + " segundos"
                            );
                            System.out.println("Atualizado em: " + estado.timestamp);
                            System.out.println("======================================");
                            System.out.println();

                            String correlationId = d.getProperties().getCorrelationId();

                            CountDownLatch latch =
                                    consultasPendentes.remove(correlationId);

                            if (latch != null) {
                                latch.countDown();
                            }

                        } else {

                            Resposta resposta =
                                    Config.GSON.fromJson(corpo, Resposta.class);

                            System.out.printf(
                                    "[%s] <- %s (%s) | maior lance: R$ %.2f | lance_id=%s%n",
                                    id,
                                    resposta.status,
                                    resposta.motivo,
                                    resposta.maiorLance,
                                    abreviar(resposta.lanceId)
                            );
                        }

                    } catch (Exception e) {
                        System.err.println(
                                "[" + id + "] Erro ao interpretar resposta: "
                                        + e.getMessage()
                        );
                    }
                },
                tag -> {}
        );

        System.out.printf(
                "[%s] Conectado ao RabbitMQ em %s:%d%n",
                id,
                Config.HOST,
                Config.PORT
        );

        System.out.println();
        System.out.println("Comandos disponíveis:");
        System.out.println("  número   -> enviar lance");
        System.out.println("  consultar -> consultar estado do leilão");
        System.out.println("  sair      -> encerrar participante");
        System.out.println();

        BufferedReader in = new BufferedReader(
                new InputStreamReader(System.in)
        );

        String linha;

        while ((linha = in.readLine()) != null) {

            linha = linha.trim();

            if (linha.equalsIgnoreCase("sair")) {
                break;
            }

            if (linha.equalsIgnoreCase("consultar")) {
                consultarEstado(
                        canalPublicacao,
                        id,
                        filaResposta
                );
                continue;
            }

            try {
                double valor = Double.parseDouble(
                        linha.replace(',', '.')
                );

                enviarLance(
                        canalPublicacao,
                        id,
                        filaResposta,
                        valor
                );

            } catch (NumberFormatException e) {
                System.out.println(
                        "Comando inválido. Digite um valor, 'consultar' ou 'sair'."
                );
            }
        }

        canalConsumo.close();
        canalPublicacao.close();
        conn.close();

        System.out.println("[" + id + "] Participante encerrado.");
    }

    private static void enviarLance(
            Channel ch,
            String id,
            String filaResposta,
            double valor
    ) throws Exception {

        if (!Double.isFinite(valor) || valor <= 0) {
            System.out.println("O lance deve ser um número positivo e válido.");
            return;
        }

        Lance lance = new Lance();

        lance.lanceId = UUID.randomUUID().toString();
        lance.itemId = Leiloeiro.ITEM;
        lance.participanteId = id;
        lance.valor = valor;
        lance.timestamp = Instant.now().toString();

        AMQP.BasicProperties props =
                new AMQP.BasicProperties.Builder()
                        .replyTo(filaResposta)
                        .correlationId(lance.lanceId)
                        .contentType("application/json")
                        .deliveryMode(2)
                        .build();

        ch.basicPublish(
                Config.EXCHANGE,
                Config.RK_LANCES,
                props,
                Config.GSON.toJson(lance)
                        .getBytes(StandardCharsets.UTF_8)
        );

        System.out.printf(
                "[%s] -> Lance de R$ %.2f enviado (lance_id=%s)%n",
                id,
                valor,
                abreviar(lance.lanceId)
        );
    }

    private static void consultarEstado(
            Channel ch,
            String id,
            String filaResposta
    ) throws Exception {

        String correlationId = UUID.randomUUID().toString();

        ConsultaEstado consulta = new ConsultaEstado();

        consulta.tipo = "CONSULTAR_ESTADO";
        consulta.itemId = Leiloeiro.ITEM;
        consulta.participanteId = id;
        consulta.timestamp = Instant.now().toString();

        CountDownLatch latch = new CountDownLatch(1);

        consultasPendentes.put(correlationId, latch);

        AMQP.BasicProperties props =
                new AMQP.BasicProperties.Builder()
                        .replyTo(filaResposta)
                        .correlationId(correlationId)
                        .contentType("application/json")
                        .deliveryMode(2)
                        .build();

        ch.basicPublish(
                Config.EXCHANGE,
                Config.RK_CONSULTAS,
                props,
                Config.GSON.toJson(consulta)
                        .getBytes(StandardCharsets.UTF_8)
        );

        System.out.println(
                "[" + id + "] -> Consulta de estado enviada ao Leiloeiro."
        );

        boolean recebeuResposta = latch.await(5, TimeUnit.SECONDS);

        if (!recebeuResposta) {
            consultasPendentes.remove(correlationId);
            System.out.println(
                    "[" + id + "] Não foi possível obter resposta do Leiloeiro."
            );
        }
    }

    private static String abreviar(String id) {
        if (id == null) {
            return "-";
        }

        return id.length() > 8 ? id.substring(0, 8) : id;
    }
}