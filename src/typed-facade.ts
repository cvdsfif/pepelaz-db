import { BigIntField, DATE_EXPECTING_NOW, DateField, DbRecord, FieldObject, FieldObjectDefinition, stringifyWithBigints, unmarshal } from "pepelaz";
import { IQueryInterface } from "./query-interfaces";

export type UpsertProps = {
    upsertFields?: string[],
    onlyReplaceNulls?: boolean
}

export interface ITypedFacade extends IQueryInterface {
    query(request: string, queryObject?: any): Promise<{ records: any[]; }>;
    typedQuery<T extends FieldObjectDefinition>(template: FieldObject<T>, request: string, queryObject?: any): Promise<{ records: DbRecord<T>[]; }>;
    multiInsert<T extends FieldObjectDefinition>(
        template: FieldObject<T>,
        tableName: string,
        records: DbRecord<T>[],
        upsertProps?: UpsertProps): Promise<DbRecord<T>[]>;
    multiUpsert<T extends FieldObjectDefinition>(
        template: FieldObject<T>,
        tableName: string,
        records: DbRecord<T>[],
        upsertProps?: UpsertProps): Promise<DbRecord<T>[]>;
    select<T extends FieldObjectDefinition>(template: FieldObject<T>, tableQuery: string, queryObject?: any): Promise<DbRecord<T>[]>;
}

const DATE_EXPECTING_NOW_TIME = DATE_EXPECTING_NOW.getTime();

class TypedFacade implements ITypedFacade {
    private db: IQueryInterface;

    constructor(db: IQueryInterface) {
        this.db = db;
    }

    private convertUppercaseIntoUnderscored = (s: String) => s.replace(/[A-Z]/g, match => `_${match.toLowerCase()}`);

    async typedQuery<T extends FieldObjectDefinition>(template: FieldObject<T> | T, request: string, queryObject?: any): Promise<{ records: DbRecord<T>[]; }> {
        return {
            records: (await this.query(request, queryObject))
                .records.map((record: any): DbRecord<T> => unmarshal(template, record))
        };
    }

    async query(request: string, queryObject?: any): Promise<{ records: any[]; }> {
        return await this.db.query(request, queryObject);
    }

    private expandTableFields = <T extends FieldObjectDefinition>(template: FieldObject<T> | T): string => {
        return Object.keys(template.definition ?? template)
            .map(key => this.convertUppercaseIntoUnderscored(key)).join(',');
    }

    private isSpecialDateValue = <T extends FieldObjectDefinition>(template: FieldObject<T>, key: string, record: any) =>
        (template.definition[key] instanceof DateField && (record[key] as Date)?.getTime() === DATE_EXPECTING_NOW_TIME);

    private indexedRecordExpansion = <T extends FieldObjectDefinition>(record: any, index: number, template: FieldObject<T>) => {
        const fillTarget = {};
        Object.keys(record).forEach(key => {
            if (!this.isSpecialDateValue(template, key, record))
                (fillTarget as any)[`${key}_${index}`] =
                    template.definition[key] instanceof BigIntField ?
                        (record[key]?.toString() ?? null) :
                        record[key]
        });
        return fillTarget;
    }

    private expandedValuesList = <T extends FieldObjectDefinition>(transactions: any[], template: FieldObject<T>) =>
        transactions.reduce((accumulator, record, index) =>
            ({ ...accumulator, ...this.indexedRecordExpansion(record, index, template) })
            , {})

    private translateTransactionFieldsIntoIndexedArguments = (record: any, index: number, template: any) =>
        Object.keys(record)
            .map(key =>
                this.isSpecialDateValue(template, key, record) ?
                    `now()` :
                    template.definition[key] instanceof BigIntField ?
                        `CAST(:${key}_${index} AS BIGINT)` :
                        `:${key}_${index}`
            ).join(",");

    private expandedArgumentsList = (records: any, template: any) =>
        records
            .map((record: any, index: number) =>
                `(${this.translateTransactionFieldsIntoIndexedArguments(record, index, template)})`)
            .join(",");

    async multiInsert<T extends FieldObjectDefinition>(
        template: FieldObject<T>,
        tableName: string,
        records: DbRecord<T>[],
        upsertProps: UpsertProps = {}): Promise<DbRecord<T>[]> {
        if (records.length == 0) return [];
        let query: string;
        let values: any;
        try {
            const upsertFields = upsertProps.upsertFields ?? null;
            query = `INSERT INTO ${tableName} AS _src(${this
                .expandTableFields(records[0])}) VALUES${this
                    .expandedArgumentsList(records, template)}${upsertFields ?
                        ` ON CONFLICT(${upsertFields
                            .join(",")}) DO UPDATE SET ${this
                                .upsertStatement(template, { upsertFields: upsertFields, onlyReplaceNulls: upsertProps.onlyReplaceNulls ?? false })}` :
                        ""}`
            values = this.expandedValuesList(records, template);
            await this.db.query(query, values);
            return records;
        } catch (err: any) {
            throw new Error(`
                    Error for the executed insert query:
                    ${query!},
                    values: ${stringifyWithBigints(values)}
                    Original error:${err.message},
                    Error stack:${err.stack}
                    `);
        }
    }

    private upsertStatement = <T extends FieldObjectDefinition>(
        template: FieldObject<T> | T,
        upsertProps: UpsertProps) => {
        return Object.keys(template.definition ?? template)
            .filter(key => (!(upsertProps.upsertFields!.includes(key))))
            .map(key => {
                const unerscoredFieldName = this.convertUppercaseIntoUnderscored(key);
                return `${unerscoredFieldName} = ${upsertProps.onlyReplaceNulls ?
                    `COALESCE(_src.${unerscoredFieldName},EXCLUDED.${unerscoredFieldName})` :
                    `EXCLUDED.${unerscoredFieldName}`}`;
            })
            .join(",");
    }

    async multiUpsert<T extends FieldObjectDefinition>(
        template: FieldObject<T>,
        tableName: string,
        records: DbRecord<T>[],
        upsertProps: UpsertProps): Promise<DbRecord<T>[]> {
        return this.multiInsert(template, tableName, records, upsertProps);
    }

    async select<T extends FieldObjectDefinition>(template: FieldObject<T>, tableQuery: string, queryObject?: any): Promise<DbRecord<T>[]> {
        return (await this.typedQuery<T>(template,
            `SELECT ${this.expandTableFields(template)} FROM ${tableQuery}`, queryObject
        )).records
    }
}

export function typedFacade(db: IQueryInterface): ITypedFacade {
    return new TypedFacade(db);
}