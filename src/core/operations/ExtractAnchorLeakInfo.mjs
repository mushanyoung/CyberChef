/**
 * @author Mushan Yang [mushan@google.com]
 * @copyright Crown Copyright 2022
 * @license Apache-2.0
 */

import OperationError from "../errors/OperationError.mjs";
import {toHexFast} from "../lib/Hex.mjs";
import Operation from "../Operation.mjs";
import Utils from "../Utils.mjs";

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
        this.description =
        "Convert Anchor Leak Info (desktop_doc_info / mobile_doc_info / sherlog log) to spanner queries.";
        this.infoURL = "";
        this.inputType = "string";
        this.outputType = "string";
        this.args = [
            {name: "ECN", type: "string", value: ""},
            {name: "Anchor Identifier", type: "string", value: ""},
            {name: "Source URL", type: "string", value: ""}, {
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
        const [ecn, anchorid, url, corpusInput] = args;
        let corpus, shards;

        const anchoridUnescaped = Utils.parseEscapedChars(anchorid);
        if (anchoridUnescaped.length !== ExpectAnchoridLength) {
            throw new OperationError(`length(Anchor Identifier)=${
                anchoridUnescaped.length}, but it must be ${ExpectAnchoridLength}`);
        }

        const ecnLen = ecn.length;
        if (ecnLen !== ExpectEcnLength) {
            throw new OperationError(
                `length(ECN)=${ecnLen}, but it must be ${ExpectEcnLength}`);
        }

        switch (corpusInput) {
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
                throw new OperationError(`Corpus=${
                    corpusInput}, but it must be one of {ramsey, mobile, web, desktop}`);
        }

        const outlinksInfoSecondaryKey =
        toHexFast(Utils.strToByteArray(`${ecn}:${anchoridUnescaped}`));
        const split = this.getRaffiaDirectoryNum(
            Utils.strToByteArray(`outlinksInfo${url}`), shards);

        const sherlogSql =
        `https://sherlog-raffia.corp.google.com/dataid?systems=raffia&config=Raffia-Prod&dataid=${
            url}`;
        const anchorDataSql =
        `$ span sql /span/global/raffia-spanner:websearch-anchors.recipe "select * from RaffiaRecords where prefix=b'anchorData' and row_key=b'${
            ecn}' and secondary_key=b'${anchorid}'"; `;
        const outlinksInfoSql = `$ span sql /span/global/raffia-spanner:${
            corpus}.recipe "select * from RaffiaRecords where prefix=b'outlinksInfo' and row_key=b'${
            url}' and secondary_key=b'outlink:${
            outlinksInfoSecondaryKey}' and split=${split};"`;
        return `----------------------\nSherlog Query:\n${
            sherlogSql}\n\nanchorData Spanner Query:\n${
            anchorDataSql}\n\noutlinksInfo Spanner Query:\n${outlinksInfoSql}`;
    }

  /**
   * Convert data to Raffia DirectoryNum.
   * Data is usually the concatenation of Raffia recipe prefix and row_key.
   * @param {byteArray|Uint8Array|ArrayBuffer} data
   * @param {Object[]} args
   * @returns {Integer}
   */
    getRaffiaDirectoryNum(data, shards = 256) {
        if (!data) return "";
        if (data instanceof ArrayBuffer) data = new Uint8Array(data);

        // Adler32
        const MOD_ADLER = 65521;
        let a = 1, b = 0;

        for (let i = 0; i < data.length; i++) {
            a += data[i];
            b += a;
        }

        a %= MOD_ADLER;
        b %= MOD_ADLER;

        const r = ((b << 16) | a) >>> 0;
        // Adler32 end

        return r % shards;
    }
}

export default ExtractAnchorLeakInfo;
