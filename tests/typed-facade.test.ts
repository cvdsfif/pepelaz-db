import { IQueryInterface } from "../src/query-interfaces";
import { typedFacade } from "../src/typed-facade";
import { extendExpectWithContainString } from "./expect-string-containing";
import { DbRecord, bigIntField, booleanField, dateField, fieldArray, fieldObject, floatField, integerField, notNull, stringField, unmarshal } from "pepelaz";

describe("Testing typed query fadace conversions", () => {
    class QueryInterfaceMock implements IQueryInterface { query = jest.fn(); }

    let dbMock: QueryInterfaceMock;

    beforeEach(() => dbMock = new QueryInterfaceMock());

    extendExpectWithContainString();

    const databaseChangeInput = fieldObject({
        creationOrder: integerField(5),
        intNotNull: integerField(notNull),
        nullableInt: integerField(),
        somethingFloat: floatField(),
        somethingBig: bigIntField(notNull),
        nullableBig: bigIntField(),
        ecriture: stringField(),
        unJour: dateField(),
        veritas: booleanField(),
        calculated: integerField(() => 2 * 2),
        explicitlyNullableInt: integerField(null),
        stringWithDefault: stringField(""),
        calculatedNullableDefault: stringField(() => null),
        calculatedNotNullableDefault: stringField(() => notNull),
        falsishBool: booleanField(),
        nullishBool: booleanField()
    });

    test("Types should be converted correctly", async () => {
        dbMock.query.mockReturnValue(Promise
            .resolve(
                {
                    records: [{
                        intNotNull: "0",
                        creationorder: "1",
                        somethingfloat: "3.456",
                        something_big: "12345678901234567890",
                        ecriture: "451",
                        un_jour: "1990-03-11",
                        veritas: true,
                        calculatedNotNullableDefault: "str"
                    }]
                }
            ));
        const record =
            (await typedFacade(dbMock).typedQuery(databaseChangeInput, "")).records[0];
        expect(record.creationOrder).toEqual(1);
        expect(record.somethingFloat).toEqual(3.456);
        expect(record.somethingBig).toEqual(BigInt("12345678901234567890"));
        expect(record.ecriture).toEqual("451");
        expect(record.unJour).toEqual(new Date("1990-03-11"));
        expect(record.veritas).toBeTruthy();
    });

    test("Should translate query with two insert values", async () => {
        const dbEntries = fieldObject({
            id: integerField(),
            someValue: stringField(),
        });
        const records = `[
            { "id": "1", "someValue": "txt" },
            { "id": 2, "someValue": "pwd" }
        ]`;
        const TABLE_NAME = "test_tab";
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiInsert(dbEntries, TABLE_NAME, unmarshalled);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(id,some_value) VALUES(:id_0,:someValue_0),(:id_1,:someValue_1)`,
            { id_0: 1, someValue_0: "txt", id_1: 2, someValue_1: "pwd" }
        )
    });

    test("Should never call subsequent queries if the arguments list is empty", async () => {
        const emptyEntries = fieldObject({});
        const records: any[] = [];
        const TABLE_NAME = "test_tab";
        await typedFacade(dbMock).multiInsert(emptyEntries, TABLE_NAME, records);
        expect(dbMock.query).not.toBeCalled();
    });

    test("Select all with fields autofill should pass", async () => {
        const intValue = 5;
        const floatValue = 5.5;
        const bigIntValue = 100n;
        const stringValue = "str";
        const dateValue = new Date("1974-03-02");
        const input = {
            creationOrder: intValue,
            nullableInt: 0,
            somethingFloat: floatValue,
            somethingBig: bigIntValue,
            nullableBig: 0n,
            ecriture: stringValue,
            unJour: dateValue,
            intNotNull: 0,
            calculatedNotNullableDefault: "str",
            veritas: true
        };
        dbMock.query.mockReturnValue({ records: [input] });
        const retval = (await typedFacade(dbMock).select(databaseChangeInput, "storage_table WHERE int_value > 0", {}))[0];
        expect(dbMock.query).toBeCalledWith(
            "SELECT creation_order,int_not_null,nullable_int,something_float,something_big,nullable_big,ecriture,un_jour,veritas,calculated,explicitly_nullable_int,string_with_default,calculated_nullable_default,calculated_not_nullable_default,falsish_bool,nullish_bool FROM storage_table WHERE int_value > 0",
            {});
        expect(retval.creationOrder).toEqual(intValue);
    });

    test("Error in multiinsert should throw an informative error", async () => {
        const dbEntries = fieldObject({
            id: integerField(),
            someValue: stringField()
        });
        const records = [
            { id: 1, someValue: "txt" },
            { id: 2, someValue: "pwd" }
        ];
        const TABLE_NAME = "test_tab";
        dbMock.query.mockImplementation(() => { throw new Error("Gluks"); });
        expect(async () => await typedFacade(dbMock).multiInsert(dbEntries, TABLE_NAME, records)).rejects.toThrow();
    });

    test("Should correctly insert false to nullable boolean fields", async () => {
        const dbEntries = fieldObject({
            bulk: booleanField()
        });
        const records = [
            { bulk: false },
        ];
        const TABLE_NAME = "test_tab";
        await typedFacade(dbMock).multiInsert(dbEntries, TABLE_NAME, records);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(bulk) VALUES(:bulk_0)`,
            { bulk_0: false }
        )
    });

    test("Should insert bigints as plain numbers", async () => {
        const dbEntries = fieldObject({
            big: bigIntField()
        });
        const records = [
            { big: 1000000000000001n },
        ];
        const TABLE_NAME = "test_tab";
        await typedFacade(dbMock).multiInsert(dbEntries, TABLE_NAME, records);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(big) VALUES(CAST(:big_0 AS BIGINT))`,
            { big_0: "1000000000000001" }
        )
    });

    test("Should be able to extract data from field objects", () => {
        const recordInput = fieldObject({ id: integerField() });
        type Record = DbRecord<typeof recordInput>;
        let rec: Record;
        rec = unmarshal(recordInput, { id: 42 });
        expect(rec.id).toBe(42);
    });

    test("Should correctly unmarshal a typical object", async () => {
        const transactionInput = fieldObject({
            id: stringField(),
            accountId: bigIntField(notNull),
            amount: bigIntField(notNull),
            reference: stringField(notNull),
            transactionType: stringField(notNull),
            transactionTs: dateField(notNull),
            runningBalance: bigIntField(notNull),
            gameId: integerField(notNull),
            tableId: bigIntField(notNull),
            sessionId: bigIntField(notNull),
            isConsolidation: booleanField()
        });
        dbMock.query.mockReturnValue({
            records: [{
                id: "WWW",
                accountId: 1n,
                amount: 100500n,
                reference: "ref",
                transactionType: "T1",
                transactionTs: "1974-03-02",
                runningBalance: 2n,
                gameId: 2,
                tableId: 3,
                sessionId: "4",
                isConsolidation: false
            }]
        });
        const result = await typedFacade(dbMock).typedQuery(transactionInput, "");
        expect(result.records[0].transactionType).toEqual("T1");
    });

    test("Multi-insert should not provide nulls for absent fields", async () => {
        const template = fieldObject({
            notNullableField: integerField(notNull),
            nullableField: integerField()
        });
        await typedFacade(dbMock).multiInsert(template, "test_table", [{
            notNullableField: 42
        }]);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO test_table AS _src(not_nullable_field) VALUES(:notNullableField_0)`,
            { notNullableField_0: 42 }
        )
    });

    test("Should normally retuerned bigint nulls", async () => {
        const transactionInput = fieldObject({
            id: stringField(),
            accountId: bigIntField(),
            amount: bigIntField(notNull),
            reference: stringField(notNull),
            transactionType: stringField(notNull),
            transactionTs: dateField(notNull),
            runningBalance: bigIntField(notNull),
            gameId: integerField(notNull),
            tableId: bigIntField(notNull),
            sessionId: bigIntField(notNull),
            isConsolidation: booleanField()
        });
        dbMock.query.mockReturnValue({
            records: [{
                id: "WWW",
                accountId: null,
                amount: 100500n,
                reference: "ref",
                transactionType: "T1",
                transactionTs: "1974-03-02",
                runningBalance: 2n,
                gameId: 2,
                tableId: 3,
                sessionId: "4",
                isConsolidation: false
            }]
        });
        const result = await typedFacade(dbMock).typedQuery(transactionInput, "");
        expect(result.records[0].accountId).toBeNull();
    });

    test("Multi-insert should correctly insert bigint nulls", async () => {
        const template = fieldObject({
            notNullableField: integerField(notNull),
            nullableField: bigIntField()
        });
        await typedFacade(dbMock).multiInsert(template, "test_table", [{
            notNullableField: 42,
            nullableField: null
        }]);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO test_table AS _src(not_nullable_field,nullable_field) VALUES(:notNullableField_0,CAST(:nullableField_0 AS BIGINT))`,
            {
                notNullableField_0: 42,
                nullableField_0: null
            }
        )
    });

    test("Should translate query with two upsert values", async () => {
        const dbEntries = fieldObject({
            id: integerField(),
            someValue: stringField(),
        });
        const records = `[
            { "id": "1", "someValue": "txt" },
            { "id": 2, "someValue": "pwd" }
        ]`;
        const TABLE_NAME = "test_tab";
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiUpsert(dbEntries, TABLE_NAME, unmarshalled, { upsertFields: ["id"] });
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(id,some_value) VALUES(:id_0,:someValue_0),(:id_1,:someValue_1) ON CONFLICT(id) DO UPDATE SET some_value = EXCLUDED.some_value`,
            { id_0: 1, someValue_0: "txt", id_1: 2, someValue_1: "pwd" }
        )
    });

    test("Should translate upsert query with two key fields with possible underscores", async () => {
        const dbEntries = fieldObject({
            idA: integerField(),
            idB: integerField(),
            someValue: stringField(),
        });
        const records = `[
            { "id_a": "1", "id_b": "1", "someValue": "txt" },
            { "id_a": 2, "id_b": "1", "someValue": "pwd" }
        ]`;
        const TABLE_NAME = "test_tab";
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiUpsert(dbEntries, TABLE_NAME, unmarshalled, { upsertFields: ["id_a", "id_b"] });
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(id_a,id_b,some_value) VALUES(:idA_0,:idB_0,:someValue_0),(:idA_1,:idB_1,:someValue_1) ON CONFLICT(id_a,id_b) DO UPDATE SET some_value = EXCLUDED.some_value`,
            { idA_0: 1, idB_0: 1, someValue_0: "txt", idA_1: 2, idB_1: 1, someValue_1: "pwd" }
        )
    });

    test("Should translate upsert query with two key fields with possible camel case", async () => {
        const dbEntries = fieldObject({
            idA: integerField(),
            idB: integerField(),
            someValue: stringField(),
        });
        const records = `[
            { "id_a": "1", "id_b": "1", "someValue": "txt" },
            { "id_a": 2, "id_b": "1", "someValue": "pwd" }
        ]`;
        const TABLE_NAME = "test_tab";
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiUpsert(dbEntries, TABLE_NAME, unmarshalled, { upsertFields: ["idA", "idB"] });
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(id_a,id_b,some_value) VALUES(:idA_0,:idB_0,:someValue_0),(:idA_1,:idB_1,:someValue_1) ON CONFLICT(id_a,id_b) DO UPDATE SET some_value = EXCLUDED.some_value`,
            { idA_0: 1, idB_0: 1, someValue_0: "txt", idA_1: 2, idB_1: 1, someValue_1: "pwd" }
        )
    });

    test("Should translate query with two upsert/replace null values", async () => {
        const dbEntries = fieldObject({
            id: integerField(),
            someValue: stringField(),
        });
        const records = `[
            { "id": "1", "someValue": "txt" },
            { "id": 2, "someValue": "pwd" }
        ]`;
        const TABLE_NAME = "test_tab";
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiUpsert(dbEntries, TABLE_NAME, unmarshalled, { upsertFields: ["id"], onlyReplaceNulls: true });
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO ${TABLE_NAME} AS _src(id,some_value) VALUES(:id_0,:someValue_0),(:id_1,:someValue_1) ON CONFLICT(id) DO UPDATE SET some_value = COALESCE(_src.some_value,EXCLUDED.some_value)`,
            { id_0: 1, someValue_0: "txt", id_1: 2, someValue_1: "pwd" }
        )
    });

    test("Insert should treat the special now value for the date fields", async () => {
        const dbEntries = fieldObject({
            dateField: dateField()
        });
        const records = `[
            { "dateField": "now" }
        ]`;
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiInsert(dbEntries, "test_table", unmarshalled);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO test_table AS _src(date_field) VALUES(now())`,
            {}
        )
    });

    test("Insert should correctly treat null values for the date fields", async () => {
        const dbEntries = fieldObject({
            dateField: dateField()
        });
        const records = `[
            { "dateField": null }
        ]`;
        const unmarshalled = unmarshal(fieldArray(dbEntries), JSON.parse(records));
        await typedFacade(dbMock).multiInsert(dbEntries, "test_table", unmarshalled);
        expect(dbMock.query).toBeCalledWith(
            `INSERT INTO test_table AS _src(date_field) VALUES(:dateField_0)`,
            { dateField_0: null }
        )
    });
})