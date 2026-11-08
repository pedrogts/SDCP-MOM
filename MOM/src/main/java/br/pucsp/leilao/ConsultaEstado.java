package br.pucsp.leilao;

import com.google.gson.annotations.SerializedName;

class ConsultaEstado {

    String tipo;

    @SerializedName("item_id")
    String itemId;

    @SerializedName("participante_id")
    String participanteId;

    String timestamp;
}