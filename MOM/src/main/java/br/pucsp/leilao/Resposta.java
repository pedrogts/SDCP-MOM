package br.pucsp.leilao;

import com.google.gson.annotations.SerializedName;

class Resposta {

    @SerializedName("lance_id")
    String lanceId;

    @SerializedName("item_id")
    String itemId;

    @SerializedName("participante_id")
    String participanteId;

    String status;
    String motivo;

    @SerializedName("maior_lance")
    Double maiorLance;

    String timestamp;
}