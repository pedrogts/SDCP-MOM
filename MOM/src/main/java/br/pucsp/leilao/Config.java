package br.pucsp.leilao;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

final class Config {
    private Config() {}

    static final String HOST = env("RABBITMQ_HOST", "localhost");
    static final int PORT = Integer.parseInt(env("RABBITMQ_PORT", "5672"));
    static final String USER = env("RABBITMQ_USER", "leilao");
    static final String PASS = env("RABBITMQ_PASS", "leilao123");

    static final String EXCHANGE = "leilao";

    static final String FILA_LANCES = "fila.lances";
    static final String RK_LANCES = "lances";

    static final String FILA_CONSULTAS = "fila.consultas";
    static final String RK_CONSULTAS = "consultas";

    static final String DLX = "leilao.dlx";
    static final String FILA_DLQ = "fila.lances.dlq";
    static final String RK_DLQ = "lances.dlq";

    static final Gson GSON = new Gson();

    static String env(String chave, String padrao) {
        String v = System.getenv(chave);
        return (v == null || v.isBlank()) ? padrao : v;
    }

    static ConnectionFactory factory() {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(HOST);
        f.setPort(PORT);
        f.setUsername(USER);
        f.setPassword(PASS);
        f.setAutomaticRecoveryEnabled(true);
        return f;
    }

    static void declararTopologia(Channel ch) throws IOException {
        // Dead letter exchange
        ch.exchangeDeclare(DLX, "direct", true);
        ch.queueDeclare(FILA_DLQ, true, false, false, null);
        ch.queueBind(FILA_DLQ, DLX, RK_DLQ);

        // Exchange principal
        ch.exchangeDeclare(EXCHANGE, "direct", true);

        // Fila de lances
        ch.queueDeclare(FILA_LANCES, true, false, false, Map.of(
                "x-dead-letter-exchange", DLX,
                "x-dead-letter-routing-key", RK_DLQ
        ));
        ch.queueBind(FILA_LANCES, EXCHANGE, RK_LANCES);

        // Fila de consultas
        ch.queueDeclare(FILA_CONSULTAS, true, false, false, null);
        ch.queueBind(FILA_CONSULTAS, EXCHANGE, RK_CONSULTAS);
    }
}