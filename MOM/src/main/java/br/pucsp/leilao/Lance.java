package br.pucsp.leilao;

import com.google.gson.annotations.SerializedName;

class Lance {

    @SerializedName("lance_id")
    String lanceId;

    @SerializedName("item_id")
    String itemId;

    @SerializedName("participante_id")
    String participanteId;

    Double valor;

    String timestamp;

    boolean valido() {
        return lanceId != null
                && !lanceId.isBlank()
                && itemId != null
                && !itemId.isBlank()
                && participanteId != null
                && !participanteId.isBlank()
                && valor != null
                && Double.isFinite(valor)
                && valor > 0;
    }
}