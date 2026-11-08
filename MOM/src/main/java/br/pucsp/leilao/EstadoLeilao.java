package br.pucsp.leilao;

import com.google.gson.annotations.SerializedName;

class EstadoLeilao {

    String tipo;

    @SerializedName("item_id")
    String itemId;

    String status;

    @SerializedName("maior_lance")
    double maiorLance;

    @SerializedName("maior_lance_de")
    String maiorLanceDe;

    @SerializedName("tempo_restante_segundos")
    long tempoRestanteSegundos;

    String timestamp;
}