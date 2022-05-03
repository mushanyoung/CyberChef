/**
 * @author Mushan Yang [mushan@google.com]
 * @copyright Crown Copyright 2022
 * @license Apache-2.0
 */

import Operation from "../Operation.mjs";
import OperationError from "../errors/OperationError.mjs";
import Utils from "../Utils.mjs";
import {toHexFast} from "../lib/Hex.mjs";

/**
 * Extract Anchor Leak Info operation
 */
class ExtractAnchorLeakInfo extends Operation {

    /**
     * ExtractAnchorLeakInfo constructor
     */
    constructor() {
        super();

        this.name = "Extract Anchor Leak Info";
        this.module = "Default";
        this.description = "Convert Anchor Leak Info (desktop_doc_info / mobile_doc_info / sherlog log) to spanner queries.";
        this.infoURL = "";
        this.inputType = "string";
        this.outputType = "string";
        this.args = [
            {
                name: "ECN",
                type: "string",
                value: ""
            },
            {
                name: "Anchor Identifier",
                type: "string",
                value: ""
            },
            {
                name: "Source URL",
                type: "string",
                value: ""
            },
            {
                name: "Corpus: ramsey (mobile) / web (desktop)",
                type: "string",
                value: "ramsey"
            }
        ];
    }

    /**
     * @param {string} input
     * @param {Object[]} args
     * @returns {string}
     */
    run(input, args) {
        const ExpectEcnLength = 24;
        const ExpectAnchoridLength = 12;
        const [ecn, anchorid, url, corpus_input] = args;
        let corpus, shards;

        const anchorid_unescaped = Utils.parseEscapedChars(anchorid);
        if (anchorid_unescaped.length != ExpectAnchoridLength) {
            throw new OperationError(`length(Anchor Identifier)=${anchorid_unescaped.length}, but it must be ${ExpectAnchoridLength}`);
        }

        const ecn_len = ecn.length;
        if (ecn_len != ExpectEcnLength) {
            throw new OperationError(`length(ECN)=${ecn_len}, but it must be ${ExpectEcnLength}`);
        }

        switch (corpus_input) {
            case "desktop":
            case "web":
                corpus = "websearch";
                shards = 256;
                break;
            case "mobile":
            case "ramsey":
                corpus = "ramsey";
                shards = 128;
                break;
            default:
                throw new OperationError(`Corpus=${corpus_input}, but it must be one of {ramsey, mobile, web, desktop}`);
        }

        const outlinksinfo_secondary_key = toHexFast(Utils.strToByteArray(`${ecn}:${anchorid_unescaped}`));
        const split = this.getRaffiaDirectoryNum(Utils.strToByteArray(`outlinksInfo${url}`), shards);

        const sherlogSql = `https://sherlog-raffia.corp.google.com/dataid?systems=raffia&config=Raffia-Prod&dataid=${url}`;
        const anchorDataSql = `$ span sql /span/global/raffia-spanner:websearch-anchors.recipe "select * from RaffiaRecords where prefix=b'anchorData' and row_key=b'${ecn}' and secondary_key=b'${anchorid}'"; `;
        const outlinksInfoSql = `$ span sql /span/global/raffia-spanner:${corpus}.recipe "select * from RaffiaRecords where prefix=b'outlinksInfo' and row_key=b'${url}' and secondary_key=b'outlink:${outlinksinfo_secondary_key}' and split=${split};"`;
        return `----------------------\nSherlog Query:\n${sherlogSql}\n\nanchorData Spanner Query:\n${anchorDataSql}\n\noutlinksInfo Spanner Query:\n${outlinksInfoSql}`;
    }

    /**
     * Convert data to Raffia DirectoryNum.
     * Data is usually the concatenation of Raffia recipe prefix and row_key.
     * @param {byteArray|Uint8Array|ArrayBuffer} data
     * @param {Object[]} args
     * @returns {Integer}
     */
    getRaffiaDirectoryNum(data, shards=256) {
        if (!data) return "";
        if (data instanceof ArrayBuffer) data = new Uint8Array(data);

        // Adler32
        const MOD_ADLER = 65521;
        let a = 1,
            b = 0;

        for (let i = 0; i < data.length; i++) {
            a += data[i];
            b += a;
        }

        const r = ((b << 16) | a) >>> 0;
        // Adler32 end

        return r % shards;
    }

}

export default ExtractAnchorLeakInfo;
