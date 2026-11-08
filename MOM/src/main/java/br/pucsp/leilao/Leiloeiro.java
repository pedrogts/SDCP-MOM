package br.pucsp.leilao;

import com.google.gson.JsonSyntaxException;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Leiloeiro {

    static final String ITEM = "item-001";

    private static double maiorLance;
    private static String maiorLanceDe = "-";

    private static long inicioLeilao;
    private static long duracaoSegundos;

    private static final Map<String, Resposta> processados =
            new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {

        maiorLance = args.length > 0
                ? Double.parseDouble(args[0].replace(',', '.'))
                : 100.0;

        duracaoSegundos = args.length > 1
                ? Long.parseLong(args[1])
                : 300;

        if (!Double.isFinite(maiorLance) || maiorLance < 0) {
            throw new IllegalArgumentException("Lance mínimo inválido.");
        }

        if (duracaoSegundos <= 0) {
            throw new IllegalArgumentException("A duração deve ser positiva.");
        }

        inicioLeilao = System.currentTimeMillis();

        Connection conn = Config.factory().newConnection("leiloeiro");
        Channel ch = conn.createChannel();

        Config.declararTopologia(ch);
        ch.basicQos(1);

        System.out.printf(
                "[Leiloeiro] Broker %s:%d | item %s | lance mínimo R$ %.2f | duração %d segundos%n",
                Config.HOST,
                Config.PORT,
                ITEM,
                maiorLance,
                duracaoSegundos
        );

        System.out.println("[Leiloeiro] Aguardando lances...");
        System.out.println("[Leiloeiro] Aguardando consultas de estado...");

        // Consumidor de lances
        DeliverCallback onLance = (tag, d) -> {
            long deliveryTag = d.getEnvelope().getDeliveryTag();

            String corpo = new String(
                    d.getBody(),
                    StandardCharsets.UTF_8
            );

            Lance lance;

            try {
                lance = Config.GSON.fromJson(corpo, Lance.class);
            } catch (JsonSyntaxException e) {
                lance = null;
            }

            if (lance == null || !lance.valido()) {
                System.out.println("[Leiloeiro] Lance inválido: " + corpo);
                ch.basicNack(deliveryTag, false, false);
                return;
            }

            // Variável final para uso na expressão lambda
            final Lance lanceRecebido = lance;

            Resposta resposta = processados.computeIfAbsent(
                    lanceRecebido.lanceId,
                    id -> avaliar(lanceRecebido)
            );

            String replyTo = d.getProperties().getReplyTo();

            if (replyTo != null) {
                String corrId = d.getProperties().getCorrelationId() != null
                        ? d.getProperties().getCorrelationId()
                        : lanceRecebido.lanceId;

                AMQP.BasicProperties props =
                        new AMQP.BasicProperties.Builder()
                                .correlationId(corrId)
                                .contentType("application/json")
                                .deliveryMode(2)
                                .build();

                ch.basicPublish(
                        "",
                        replyTo,
                        props,
                        Config.GSON.toJson(resposta)
                                .getBytes(StandardCharsets.UTF_8)
                );
            }

            ch.basicAck(deliveryTag, false);
        };

        // Consumidor de consultas
        DeliverCallback onConsulta = (tag, d) -> {
            long deliveryTag = d.getEnvelope().getDeliveryTag();

            String replyTo = d.getProperties().getReplyTo();
            String correlationId = d.getProperties().getCorrelationId();

            try {
                String corpo = new String(
                        d.getBody(),
                        StandardCharsets.UTF_8
                );

                ConsultaEstado consulta =
                        Config.GSON.fromJson(corpo, ConsultaEstado.class);

                if (consulta == null
                        || !"CONSULTAR_ESTADO".equals(consulta.tipo)
                        || !ITEM.equals(consulta.itemId)) {

                    System.out.println("[Leiloeiro] Consulta inválida: " + corpo);
                    ch.basicAck(deliveryTag, false);
                    return;
                }

                if (replyTo == null) {
                    System.out.println("[Leiloeiro] Consulta sem reply_to.");
                    ch.basicAck(deliveryTag, false);
                    return;
                }

                EstadoLeilao estado = consultarEstado();

                AMQP.BasicProperties props =
                        new AMQP.BasicProperties.Builder()
                                .correlationId(correlationId)
                                .contentType("application/json")
                                .deliveryMode(2)
                                .build();

                ch.basicPublish(
                        "",
                        replyTo,
                        props,
                        Config.GSON.toJson(estado)
                                .getBytes(StandardCharsets.UTF_8)
                );

                System.out.printf(
                        "[Leiloeiro] Consulta respondida para %s | maior lance: R$ %.2f | restante: %d s%n",
                        consulta.participanteId,
                        estado.maiorLance,
                        estado.tempoRestanteSegundos
                );

                ch.basicAck(deliveryTag, false);

            } catch (Exception e) {
                System.err.println("[Leiloeiro] Erro ao processar consulta: " + e.getMessage());
                ch.basicNack(deliveryTag, false, false);
            }
        };

        ch.basicConsume(
                Config.FILA_LANCES,
                false,
                onLance,
                tag -> {}
        );

        ch.basicConsume(
                Config.FILA_CONSULTAS,
                false,
                onConsulta,
                tag -> {}
        );

        System.out.println("[Leiloeiro] Em execução. Pressione Ctrl+C para encerrar.");
    }

    private static synchronized Resposta avaliar(Lance l) {

        Resposta r = new Resposta();

        r.lanceId = l.lanceId;
        r.itemId = l.itemId;
        r.participanteId = l.participanteId;
        r.timestamp = Instant.now().toString();

        if (!ITEM.equals(l.itemId)) {
            r.status = "RECUSADO";
            r.motivo = "Item inexistente";

        } else if (leilaoEncerrado()) {
            r.status = "RECUSADO";
            r.motivo = "Leilão encerrado";

        } else if (l.valor > maiorLance) {
            maiorLance = l.valor;
            maiorLanceDe = l.participanteId;

            r.status = "ACEITO";
            r.motivo = "Maior lance vigente";

        } else {
            r.status = "RECUSADO";
            r.motivo = "Valor não supera o maior lance atual";
        }

        r.maiorLance = maiorLance;

        System.out.printf(
                "[Leiloeiro] %s deu R$ %.2f -> %s (maior lance: R$ %.2f de %s)%n",
                l.participanteId,
                l.valor,
                r.status,
                maiorLance,
                maiorLanceDe
        );

        return r;
    }

    private static synchronized EstadoLeilao consultarEstado() {

        EstadoLeilao estado = new EstadoLeilao();

        long decorrido =
                (System.currentTimeMillis() - inicioLeilao) / 1000;

        long restante = Math.max(0, duracaoSegundos - decorrido);

        estado.tipo = "ESTADO_LEILAO";
        estado.itemId = ITEM;
        estado.status = restante > 0 ? "ABERTO" : "ENCERRADO";
        estado.maiorLance = maiorLance;
        estado.maiorLanceDe = maiorLanceDe;
        estado.tempoRestanteSegundos = restante;
        estado.timestamp = Instant.now().toString();

        return estado;
    }

    private static boolean leilaoEncerrado() {
        return (System.currentTimeMillis() - inicioLeilao)
                >= duracaoSegundos * 1000;
    }
}